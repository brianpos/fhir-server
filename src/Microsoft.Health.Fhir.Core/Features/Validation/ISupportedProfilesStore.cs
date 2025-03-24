// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Core.Features.Validation
{
    public interface ISupportedProfilesStore
    {
        /// <summary>
        /// Provide supported profiles for specified <paramref name="resourceType"/>.
        /// </summary>
        /// <param name="resourceType">Resource type to get profiles.</param>
        /// <param name="disableCacheRefresh">Should we check server for new updates or get data out of cache.</param>
        IEnumerable<string> GetSupportedProfiles(string resourceType, bool disableCacheRefresh = false);

        void Refresh();

        /// <summary>
        /// Refresh any data associated with the referenced resource
        /// </summary>
        /// <param name="canonicalUrl">The canonical URL of the resource that was changed</param>
        /// <param name="canonicalVersion">The canonical Version of the resource that was changed</param>
        /// <param name="resourceType">The resource type of the resource that was changed</param>
        /// <param name="resourceId">The resource Id of the resource that was changed</param>
        /// <param name="resource">The actual resource that was changed</param>
        /// <returns>async task</returns>
        System.Threading.Tasks.Task Refresh(string canonicalUrl, string canonicalVersion, string resourceType, string resourceId, Models.ResourceElement resource);

        /// <summary>
        /// Delete any data associated with the referenced resource
        /// </summary>
        /// <param name="resourceType">The resource type of the resource that was deleted</param>
        /// <param name="resourceId">The resource Id of the resource that was deleted</param>
        /// <returns>async task</returns>
        System.Threading.Tasks.Task Delete(string resourceType, string resourceId);
    }
}
