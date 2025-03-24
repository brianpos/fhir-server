// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using Hl7.Fhir.Specification.Source;
using Hl7.Fhir.Specification.Summary;
using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Core.Extensions;
using Microsoft.Health.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Core.Configs;
using Microsoft.Health.Fhir.Core.Features.Search;
using Microsoft.Health.Fhir.Core.Messages.CapabilityStatement;
using Microsoft.Health.Fhir.Core.Models;

namespace Microsoft.Health.Fhir.Core.Features.Validation
{
    /// <summary>
    /// Provides profiles by fetching them from the server.
    /// </summary>
    public sealed class ServerProvideProfileValidation : IProvideProfilesForValidation, IDisposable
    {
        private static HashSet<string> _supportedTypes = new HashSet<string>() { "ValueSet", "StructureDefinition", "CodeSystem" };
        private readonly Func<IScoped<ISearchService>> _searchServiceFactory;
        private readonly ValidateOperationConfiguration _validateOperationConfig;
        private readonly IMediator _mediator;
        private readonly ILogger<ServerProvideProfileValidation> _logger;
        private List<ArtifactSummary> _summaries = new List<ArtifactSummary>();
        private DateTime _expirationTime;
        private SemaphoreSlim _lockSummaries = new SemaphoreSlim(1, 1);

        private const string ArtifactSummaryPropertiesResourceIdKey = "ResourceId";

        public ServerProvideProfileValidation(
            Func<IScoped<ISearchService>> searchServiceFactory,
            IOptions<ValidateOperationConfiguration> options,
            IMediator mediator,
            ILogger<ServerProvideProfileValidation> logger)
        {
            EnsureArg.IsNotNull(searchServiceFactory, nameof(searchServiceFactory));
            EnsureArg.IsNotNull(options?.Value, nameof(options));
            EnsureArg.IsNotNull(mediator, nameof(mediator));

            _searchServiceFactory = searchServiceFactory;
            _expirationTime = DateTime.UtcNow;
            _validateOperationConfig = options.Value;
            _mediator = mediator;
            _logger = logger;
        }

        public IReadOnlySet<string> GetProfilesTypes() => _supportedTypes;

        public void Refresh()
        {
            _expirationTime = DateTime.UtcNow.AddMilliseconds(-1);
            ListSummaries();
        }

        public async System.Threading.Tasks.Task Refresh(string canonicalUrl, string canonicalVersion, string resourceType, string resourceId, Models.ResourceElement resource)
        {
            _expirationTime = DateTime.UtcNow.AddMilliseconds(-1);

            // Instead of just reloading all the summaries from disk, patch the resource into the _summaries
            var oldHash = GetHashForSupportedProfiles(_summaries);

            // The summaries cache is populated with Artifact summaries which must be scanned from the raw JSON
            // content, so need to scan the resource as Json content
            // Process out the content from the request
            string rawJson = await resource.Instance.ToJsonAsync();
            ArtifactSummaryGenerator generator;
            List<ArtifactSummary> artifacts;
#if Stu3
            generator = ArtifactSummaryGenerator.Default;
#else
            generator = new ArtifactSummaryGenerator(ModelInfo.ModelInspector);
#endif
            using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(rawJson)))
            {
                using var navStream = new JsonNavigatorStream(memoryStream);
                void SetOrigin(ArtifactSummaryPropertyBag properties)
                {
                    properties[ArtifactSummaryProperties.OriginKey] = rawJson;
                    properties[ArtifactSummaryPropertiesResourceIdKey] = resourceId;
                }

                artifacts = generator.Generate(navStream, SetOrigin);
            }

            // Check that the item is in the cache
            if (await _lockSummaries.WaitAsync(TimeSpan.FromSeconds(10)))
            {
                try
                {
                    if (!string.IsNullOrEmpty(canonicalUrl))
                    {
                        // This is not canonical version resilient, but will be in the next version
                        // var existingEntries = _summaries.Where(s => s.GetConformanceCanonicalUrl() == canonicalUrl && s.GetConformanceVersion() == canonicalVersion).ToList();
                        var existingEntries = _summaries.Where(s => s.GetConformanceCanonicalUrl() == canonicalUrl).ToList();
                        if (existingEntries.Count > 0)
                        {
                            // remove the old ones (version aware)
                            _summaries.RemoveAll(s => existingEntries.Contains(s));
                        }

                        // Add in the loaded artifacts
                        _summaries.AddRange(artifacts);
                    }
                    else
                    {
                        // This is an update to a canonical resource with no canonical URL, so remove from the cache if it exists, by Type/ID.
                        var existingEntries = _summaries.Where(s => s.ResourceTypeName == resourceType && s.GetValueOrDefault<string>(ArtifactSummaryPropertiesResourceIdKey) == resourceId).ToList();
                        if (existingEntries.Count > 0)
                        {
                            // remove the resource
                            _summaries.RemoveAll(s => existingEntries.Contains(s));
                        }
                    }
                }
                finally
                {
                    _lockSummaries.Release();
                }
            }
            else
            {
                // The lock timed out, this should be logged, as this is a potential deadlock occurrence
                _logger.LogError($"Timed out waiting for the lock to refresh {canonicalUrl}|{canonicalVersion} ({resourceType}/{resourceId}) in the summaries.");
            }

            // This is the part that is deadlocking
            // var result = await GetSummaries();
            // _summaries = result;
            var newHash = GetHashForSupportedProfiles(_summaries);
            _expirationTime = DateTime.UtcNow.AddSeconds(_validateOperationConfig.CacheDurationInSeconds);

            if (newHash != oldHash)
            {
                await _mediator.Publish(new RebuildCapabilityStatement(RebuildPart.Profiles));
            }
        }

        public async System.Threading.Tasks.Task Delete(string resourceType, string resourceId)
        {
            _expirationTime = DateTime.UtcNow.AddMilliseconds(-1);

            // Instead of just reloading all the summaries from disk, patch the resource into the _summaries
            var oldHash = GetHashForSupportedProfiles(_summaries);

            // Check that the item is in the cache
            if (await _lockSummaries.WaitAsync(TimeSpan.FromSeconds(10)))
            {
                try
                {
                    // This is the removal of a canonical resource, these are done by ID.
                    var existingEntries = _summaries.Where(s => s.ResourceTypeName == resourceType && s.GetValueOrDefault<string>(ArtifactSummaryPropertiesResourceIdKey) == resourceId).ToList();
                    if (existingEntries.Count > 0)
                    {
                        // remove the resource
                        _summaries.RemoveAll(s => existingEntries.Contains(s));
                    }
                }
                finally
                {
                    _lockSummaries.Release();
                }
            }
            else
            {
                // The lock timed out, this should be logged, as this is a potential deadlock occurrence
                _logger.LogError($"Timed out waiting for the lock to delete ({resourceType}/{resourceId}) in the summaries.");
            }

            // This is the part that is deadlocking
            // var result = await GetSummaries();
            // _summaries = result;
            var newHash = GetHashForSupportedProfiles(_summaries);
            _expirationTime = DateTime.UtcNow.AddSeconds(_validateOperationConfig.CacheDurationInSeconds);

            if (newHash != oldHash)
            {
                await _mediator.Publish(new RebuildCapabilityStatement(RebuildPart.Profiles));
            }
        }

        public IEnumerable<ArtifactSummary> ListSummaries(bool resetStatementIfNew = true, bool disablePull = false)
        {
            if (disablePull)
            {
                return _summaries;
            }

            if (_lockSummaries.Wait(TimeSpan.FromSeconds(10)))
            {
                try
                {
                    if (_expirationTime < DateTime.UtcNow)
                    {
                        var oldHash = resetStatementIfNew ? GetHashForSupportedProfiles(_summaries) : string.Empty;

                        // This is the part that is deadlocking
                        List<ArtifactSummary> result = GetSummaries().GetAwaiter().GetResult();
                        _summaries = result;
                        var newHash = resetStatementIfNew ? GetHashForSupportedProfiles(_summaries) : string.Empty;
                        _expirationTime = DateTime.UtcNow.AddSeconds(_validateOperationConfig.CacheDurationInSeconds);

                        if (newHash != oldHash)
                        {
                            _mediator.Publish(new RebuildCapabilityStatement(RebuildPart.Profiles)).GetAwaiter().GetResult();
                        }
                    }
                }
                finally
                {
                    _lockSummaries.Release();
                }
            }
            else
            {
                // The lock timed out, this should be logged, as this is a potential deadlock occurrence
                _logger.LogError("Timed out waiting for the lock to refresh the summaries.");
            }

            return _summaries;
        }

        private async Task<List<ArtifactSummary>> GetSummaries()
        {
            var result = new Dictionary<string, ArtifactSummary>();
            using (IScoped<ISearchService> searchService = _searchServiceFactory())
            {
                foreach (var type in _supportedTypes)
                {
                    string ct = null;
                    {
                        do
                        {
                            var queryParameters = new List<Tuple<string, string>>();
                            if (ct != null)
                            {
                                ct = ContinuationTokenEncoder.Encode(ct);
                                queryParameters.Add(new Tuple<string, string>(KnownQueryParameterNames.ContinuationToken, ct));
                            }

                            SearchResult searchResult = await searchService.Value.SearchAsync(type, queryParameters, CancellationToken.None);
                            foreach (SearchResultEntry searchItem in searchResult.Results)
                            {
                                using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(searchItem.Resource.RawResource.Data)))
                                {
                                    using var navStream = new JsonNavigatorStream(memoryStream);
                                    void SetOrigin(ArtifactSummaryPropertyBag properties)
                                    {
                                        properties[ArtifactSummaryProperties.OriginKey] = searchItem.Resource.RawResource.Data;
                                        properties[ArtifactSummaryPropertiesResourceIdKey] = searchItem.Resource.ResourceId;
                                    }

#if Stu3
                                    List<ArtifactSummary> artifacts = ArtifactSummaryGenerator.Default.Generate(navStream, SetOrigin);
#else
                                    List<ArtifactSummary> artifacts = new ArtifactSummaryGenerator(ModelInfo.ModelInspector).Generate(navStream, SetOrigin);
#endif

                                    foreach (ArtifactSummary artifact in artifacts)
                                    {
                                        result[artifact.ResourceUri] = artifact;
                                    }
                                }
                            }

                            ct = searchResult.ContinuationToken;
                        }
                        while (ct != null);
                    }
                }

                return result.Values.ToList();
            }
        }

        private static Resource LoadBySummary(ArtifactSummary summary)
        {
            if (summary == null)
            {
                return null;
            }

            using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(summary.Origin)))
            using (var navStream = new JsonNavigatorStream(memoryStream))
            {
                if (navStream.Seek(summary.Position))
                {
                    if (navStream.Current != null)
                    {
                        // TODO: Cache this parsed resource, to prevent parsing again and again
                        Resource resource = navStream.Current.ToPoco<Resource>();
                        return resource;
                    }
                }
            }

            return null;
        }

        public Resource ResolveByCanonicalUri(string uri)
        {
            ArtifactSummary summary = ListSummaries().ResolveByCanonicalUri(uri);
            return LoadBySummary(summary);
        }

        public Resource ResolveByUri(string uri)
        {
            ArtifactSummary summary = ListSummaries().ResolveByUri(uri);
            return LoadBySummary(summary);
        }

        public IEnumerable<string> GetSupportedProfiles(string resourceType, bool disableCacheRefresh = false)
        {
            IEnumerable<ArtifactSummary> summary = ListSummaries(false, disableCacheRefresh);
            return summary.Where(x => x.ResourceTypeName == KnownResourceTypes.StructureDefinition)
                .Where(x =>
                    {
                        if (!x.TryGetValue(StructureDefinitionSummaryProperties.TypeKey, out object type))
                        {
                            return false;
                        }

                        return string.Equals((string)type, resourceType, StringComparison.OrdinalIgnoreCase);
                    })
                .Select(x => x.ResourceUri).ToList();
        }

        private static string GetHashForSupportedProfiles(IReadOnlyCollection<ArtifactSummary> summaries)
        {
            if (summaries == null)
            {
                return string.Empty;
            }

            var sb = new StringBuilder();
            summaries.Where(x => x.ResourceTypeName == KnownResourceTypes.StructureDefinition)
               .Where(x => x.TryGetValue(StructureDefinitionSummaryProperties.TypeKey, out object type))
               .Select(x => x.ResourceUri).ToList().ForEach(url => sb.Append(url));

            return sb.ToString().ComputeHash();
        }

        public void Dispose()
        {
            _lockSummaries.Dispose();
        }
    }
}
