﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading.Tasks;
using Hl7.Fhir.Model;
using Microsoft.Health.Fhir.Client;
using Microsoft.Health.Fhir.Core.Extensions;
using Microsoft.Health.Fhir.Core.Features;
using Microsoft.Health.Fhir.Core.Models;
using Microsoft.Health.Fhir.Tests.Common;
using Microsoft.Health.Fhir.Tests.Common.FixtureParameters;
using Microsoft.Health.Test.Utilities;
using Xunit;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.Tests.E2E.Rest.Search
{
    [Trait(Traits.OwningTeam, OwningTeam.Fhir)]
    [Trait(Traits.Category, Categories.Search)]
    [HttpIntegrationFixtureArgumentSets(DataStore.All, Format.Json)]
    public class ChainingSearchTests : SearchTestsBase<ChainingSearchTests.ClassFixture>
    {
        public ChainingSearchTests(ClassFixture fixture)
            : base(fixture)
        {
            if (!Client.HttpClient.DefaultRequestHeaders.Contains(KnownHeaders.EnableChainSearch))
            {
                Client.HttpClient.DefaultRequestHeaders.TryAddWithoutValidation(KnownHeaders.EnableChainSearch, "true");
            }
        }

        [Fact]
        public async Task GivenAChainedSearchExpression_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&subject:Patient.name={Fixture.SmithPatientGivenName}";

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);

            ValidateBundle(bundle, Fixture.SmithSnomedDiagnosticReport, Fixture.SmithLoincDiagnosticReport);
        }

        [Fact]
        public async Task GivenANestedChainedSearchExpression_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&result.subject:Patient.name={Fixture.SmithPatientGivenName}";

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);

            ValidateBundle(bundle, Fixture.SmithSnomedDiagnosticReport, Fixture.SmithLoincDiagnosticReport);
        }

        [Fact]
        public async Task GivenAMultiNestedChainedSearchExpression_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&result.subject:Patient.organization.address-city={Fixture.OrganizationCity}";

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);

            ValidateBundle(bundle, Fixture.SmithSnomedDiagnosticReport, Fixture.SmithLoincDiagnosticReport);
        }

        [Fact]
        public async Task GivenANestedChainedSearchExpressionWithAnOrFinalCondition_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&result.subject:Patient.name={Fixture.SmithPatientGivenName},{Fixture.TrumanPatientGivenName}";

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);

            ValidateBundle(bundle, Fixture.SmithSnomedDiagnosticReport, Fixture.SmithLoincDiagnosticReport, Fixture.TrumanSnomedDiagnosticReport, Fixture.TrumanLoincDiagnosticReport);
        }

        [Fact]
        public async Task GivenAChainedSearchExpressionOverASimpleParameter_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&subject:Patient._tag={Fixture.Tag}";

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);

            ValidateBundle(bundle, Fixture.SmithSnomedDiagnosticReport, Fixture.SmithLoincDiagnosticReport, Fixture.TrumanSnomedDiagnosticReport, Fixture.TrumanLoincDiagnosticReport);
        }

        [Fact]
        public async Task GivenAChainedSearchExpressionOverASimpleParameter_WhenSearchedWithPaging_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&subject:Patient._tag={Fixture.Tag}&_count=2";

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);

            ValidateBundle(bundle, Fixture.SmithSnomedDiagnosticReport, Fixture.TrumanSnomedDiagnosticReport);

            bundle = await Client.SearchAsync(bundle.NextLink.ToString());

            ValidateBundle(bundle, Fixture.SmithLoincDiagnosticReport, Fixture.TrumanLoincDiagnosticReport);
        }

        [Fact]
        public async Task GivenAChainedSearchExpressionOverASimpleParameterWithNoResults_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&subject:Patient._type=Observation";

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);

            ValidateBundle(bundle);
        }

        [Fact]
        public async Task GivenAReverseChainSearchExpressionOverASimpleParameter_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&_has:Observation:patient:code={Fixture.SnomedCode}";

            Bundle bundle = await Client.SearchAsync(ResourceType.Patient, query);

            ValidateBundle(bundle, Fixture.SmithPatient, Fixture.TrumanPatient);
        }

        [Fact]
        public async Task GivenAReverseChainSearchExpressionOverASimpleParameter_WhenSearchedWithPaging_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&_has:Observation:patient:code={Fixture.SnomedCode}&_count=1";

            Bundle bundle = await Client.SearchAsync(ResourceType.Patient, query);

            ValidateBundle(bundle, Fixture.SmithPatient);

            bundle = await Client.SearchAsync(bundle.NextLink.ToString());

            ValidateBundle(bundle, Fixture.TrumanPatient);
        }

        [Fact]
        public async Task GivenAReverseChainSearchExpressionWithMultipleTargetTypes_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"?_tag={Fixture.Tag}&_type=Patient,Device&_has:Observation:subject:code={Fixture.SnomedCode}";

            Bundle bundle = await Client.SearchAsync(query);

            ValidateBundle(bundle, Fixture.SmithPatient, Fixture.TrumanPatient, Fixture.DeviceSnomedSubject);
        }

        [Fact]
        public async Task GivenANestedReverseChainSearchExpressionOverASimpleParameter_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&_has:Observation:patient:_has:DiagnosticReport:result:code={Fixture.SnomedCode}";

            Bundle bundle = await Client.SearchAsync(ResourceType.Patient, query);

            ValidateBundle(bundle, Fixture.SmithPatient, Fixture.TrumanPatient);
        }

        [Fact]
        public async Task GivenANestedReverseChainSearchExpressionOverTheIdResourceParameter_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&_has:Group:member:_id={Fixture.PatientGroup.Id}";

            Bundle bundle = await Client.SearchAsync(ResourceType.Patient, query);

            ValidateBundle(bundle, Fixture.AdamsPatient, Fixture.SmithPatient, Fixture.TrumanPatient);
        }

        [HttpIntegrationFixtureArgumentSets(DataStore.SqlServer, Format.Json)]
        [Fact]
        public async Task GivenANestedReverseChainSearchExpressionOverTheTypeResourceParameter_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            Bundle bundle = await Client.SearchAsync(ResourceType.Patient, $"_tag={Fixture.Tag}&_has:Group:member:_type=Group");

            Assert.NotEmpty(bundle.Entry);

            bundle = await Client.SearchAsync(ResourceType.Patient, $"_tag={Fixture.Tag}&_has:Group:member:_type=Patient");

            Assert.Empty(bundle.Entry);
        }

        [HttpIntegrationFixtureArgumentSets(DataStore.SqlServer, Format.Json)]
        [Fact]
        public async Task GivenAChainedSearchExpressionWithAPredicateOnSurrogateId_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"subject:Patient._type=Patient&subject:Patient._tag={Fixture.Tag}";

            Bundle completeBundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);
            Assert.True(completeBundle.Entry.Count > 2);

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query, count: 1);
            List<Bundle.EntryComponent> resources = new List<Bundle.EntryComponent>();
            resources.AddRange(bundle.Entry);
            while (bundle.NextLink != null)
            {
                bundle = await Client.SearchAsync(bundle.NextLink.ToString());
                resources.AddRange(bundle.Entry);
            }

            ValidateBundle(new Bundle { Entry = resources }, completeBundle.Entry.Select(e => e.Resource).ToArray());
        }

        [Fact]
        public async Task GivenAReverseChainedSearchExpressionWithAPredicateOnSurrogateId_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_has:Observation:patient:_tag={Fixture.Tag}";

            Bundle completeBundle = await Client.SearchAsync(ResourceType.Patient, query);
            Assert.True(completeBundle.Entry.Count > 2);

            Bundle bundle = await Client.SearchAsync(ResourceType.Patient, query, count: 1);
            List<Bundle.EntryComponent> resources = new List<Bundle.EntryComponent>();
            resources.AddRange(bundle.Entry);
            while (bundle.NextLink != null)
            {
                bundle = await Client.SearchAsync(bundle.NextLink.ToString());
                resources.AddRange(bundle.Entry);
            }

            ValidateBundle(new Bundle { Entry = resources }, completeBundle.Entry.Select(e => e.Resource).ToArray());
        }

        [Fact]
        public async Task GivenACombinationOfChainingReverseChainSearchExpressionOverASimpleParameter_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&code={Fixture.SnomedCode}&patient:Patient._has:Group:member:_tag={Fixture.Tag}";

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);

            ValidateBundle(bundle, Fixture.SmithSnomedDiagnosticReport, Fixture.TrumanSnomedDiagnosticReport);
        }

        [Fact]
        public async Task GivenACombinationOfChainingReverseChainSearchExpressionOverASimpleParameter_WhenSearchedWithPaging_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&code={Fixture.SnomedCode}&patient:Patient._has:Group:member:_tag={Fixture.Tag}&_count=1";

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);

            ValidateBundle(bundle, Fixture.SmithSnomedDiagnosticReport);

            bundle = await Client.SearchAsync(bundle.NextLink.ToString());

            ValidateBundle(bundle, Fixture.TrumanSnomedDiagnosticReport);
        }

        [Fact]
        public async Task GivenACombinationOfChainingReverseChainSearchExpressionOverAResourceTableParameter_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&code={Fixture.SnomedCode}&patient:Patient._has:Group:member:_id={Fixture.PatientGroup.Id}";

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);

            ValidateBundle(bundle, Fixture.SmithSnomedDiagnosticReport, Fixture.TrumanSnomedDiagnosticReport);
        }

        [Fact]
        public async Task GivenACombinationOfChainingReverseChainSearchExpressionOverAResourceTableParameter_WhenSearchedWithPaging_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"_tag={Fixture.Tag}&code={Fixture.SnomedCode}&patient:Patient._has:Group:member:_id={Fixture.PatientGroup.Id}&_count=1";

            Bundle bundle = await Client.SearchAsync(ResourceType.DiagnosticReport, query);

            ValidateBundle(bundle, Fixture.SmithSnomedDiagnosticReport);

            bundle = await Client.SearchAsync(bundle.NextLink.ToString());

            ValidateBundle(bundle, Fixture.TrumanSnomedDiagnosticReport);
        }

        [HttpIntegrationFixtureArgumentSets(DataStore.CosmosDb, Format.Json)]
        [Fact]
        public async Task GivenANonSelectiveChainingQueryInCosmosDb_WhenSearched_ThenAnErrorShouldBeThrown()
        {
            string tag = Guid.NewGuid().ToString();
            string query = $"_tag={tag}&subject:Patient.gender=male";

            Patient resource = Samples.GetJsonSample("Patient-f001").ToPoco<Patient>();
            resource.Meta = new Meta()
            {
                Tag = new List<Coding>()
                {
                    new Coding("testTag", tag),
                },
            };

            try
            {
                // Safe number to create resources without hitting any Cosmos DB limit, as the number of RUs assigned is low.
                const int batchSize = 10;

                // Create 1001 patients that exceed the sub-query limit
                foreach (IEnumerable<int> batch in Enumerable.Range(1, 1001).TakeBatch(batchSize))
                {
                    await Task.WhenAll(batch.Select(_ => Client.CreateAsync(resource)));
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed while creating resources: {ex.Message}", ex);
            }

            try
            {
                await Client.SearchAsync(ResourceType.Observation, query);

                Assert.Fail("Test was expected to fail with a FhirException.");
            }
            catch (FhirClientException fce)
            {
                const string expectedMessage = "Sub-queries in a chained expression cannot return more than 1000 results";

                Assert.True(
                    fce.Message.Contains(expectedMessage),
                    $"FhirClientException received is different than the one expected: Message: {fce.Message}");
            }
            catch (Exception ex)
            {
                Assert.Fail($"Unexpected exception thrown: {ex}");
            }
        }

        [HttpIntegrationFixtureArgumentSets(DataStore.SqlServer, Format.Json)]
        [Fact]
        public async Task GivenAChainedSearchExpressionWithNotProvider_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"subject:Patient.gender:not=female&subject:Patient._tag={Fixture.Tag}";

            Bundle notFemaleBundle = await Client.SearchAsync(ResourceType.Observation, query);
            Assert.True(notFemaleBundle.Entry.Count == 4);

            query = $"subject:Patient.gender:not=male&subject:Patient._tag={Fixture.Tag}";

            Bundle notMaleBundle = await Client.SearchAsync(ResourceType.Observation, query);
            Assert.True(notMaleBundle.Entry.Count == 1);

            query = $"subject:Patient._tag={Fixture.Tag}";

            Bundle allPatientBundle = await Client.SearchAsync(ResourceType.Observation, query);
            Assert.True(allPatientBundle.Entry.Count == notMaleBundle.Entry.Count + notFemaleBundle.Entry.Count);
        }

#if !Stu3
        [HttpIntegrationFixtureArgumentSets(DataStore.SqlServer, Format.Json)]
        [Fact]
        public async Task GivenTwoChainedSearchExpressionsAndInclude_WhenSearched_ThenCorrectBundleShouldBeReturned()
        {
            string query = $"participating-organization.identifier={Fixture.OrganizationIdentifier}&_include=OrganizationAffiliation:location&participating-organization.type=practice";

            Bundle bundle = await Client.SearchAsync(ResourceType.OrganizationAffiliation, query);
            Assert.Equal(2, bundle.Entry.Count);
        }
#endif

        [Fact]
        public async Task GivenCountOnlyReverseChainSearchWithDeletedResource_WhenSearched_ThenCorrectCountIsReturned()
        {
            string query = $"_has:CareTeam:patient:_tag={Fixture.Tag}&_summary=count";

            Bundle bundle = await Client.SearchAsync(ResourceType.Patient, query);
            Assert.Equal(1, bundle.Total);
        }

        public class ClassFixture : HttpIntegrationTestFixture
        {
            public ClassFixture(DataStore dataStore, Format format, TestFhirServerFactory testFhirServerFactory)
                : base(dataStore, format, testFhirServerFactory)
            {
            }

            public Group PatientGroup { get; private set; }

            public string Tag { get; private set; }

            public Patient AdamsPatient { get; private set; }

            public Patient TrumanPatient { get; private set; }

            public DiagnosticReport TrumanSnomedDiagnosticReport { get; private set; }

            public DiagnosticReport TrumanLoincDiagnosticReport { get; private set; }

            public string SmithPatientGivenName { get; } = Guid.NewGuid().ToString();

            public string TrumanPatientGivenName { get; } = Guid.NewGuid().ToString();

            public string SnomedCode { get; } = Guid.NewGuid().ToString();

            public string OrganizationCity { get; } = Guid.NewGuid().ToString();

            public string OrganizationIdentifier { get; } = Guid.NewGuid().ToString();

            public Patient SmithPatient { get; private set; }

            public DiagnosticReport SmithSnomedDiagnosticReport { get; private set; }

            public DiagnosticReport SmithLoincDiagnosticReport { get; private set; }

            public Device DeviceLoincSubject { get; private set; }

            public Device DeviceSnomedSubject { get; private set; }

            protected override async Task OnInitializedAsync()
            {
                Tag = Guid.NewGuid().ToString();

                // Construct an observation pointing to a patient and a diagnostic report pointing to the observation and the patient along with some not matching entries
                var snomedCode = new CodeableConcept("http://snomed.info/sct", SnomedCode);
                var loincCode = new CodeableConcept("http://loinc.org", "4548-4");

                var meta = new Meta
                {
                    Tag = new List<Coding>
                    {
                        new Coding("testTag", Tag),
                    },
                };

#if Stu3 || R4 || R4B
                var organization = (await TestFhirClient.CreateAsync(new Organization { Meta = meta, Identifier = new() { new Identifier(null, OrganizationIdentifier) }, Address = new List<Address> { new() { City = "Seattle" }, new() { City = OrganizationCity } }, Type = new() { new CodeableConcept(null, "practice") } })).Resource;
#else
                var organization = (await TestFhirClient.CreateAsync(new Organization { Meta = meta, Identifier = new() { new Identifier(null, OrganizationIdentifier) }, Contact = new() { new() { Address = new() { City = "Seattle" } }, new() { Address = new() { City = OrganizationCity } } }, Type = new() { new CodeableConcept(null, "practice") } })).Resource;
#endif

                var location = (await TestFhirClient.CreateAsync(new Location { Meta = meta, Address = new Address { City = "Seattle" } })).Resource;

#if !Stu3
                var affiliateOrganization = (await TestFhirClient.CreateAsync(new OrganizationAffiliation { Meta = meta, ParticipatingOrganization = new ResourceReference($"{KnownResourceTypes.Organization}/{organization.Id}"), Location = new() { new ResourceReference($"{KnownResourceTypes.Location}/{location.Id}") }})).Resource;
#endif

                AdamsPatient = (await TestFhirClient.CreateAsync(new Patient { Meta = meta, Gender = AdministrativeGender.Female, Name = new List<HumanName> { new HumanName { Family = "Adams" } } })).Resource;
                SmithPatient = (await TestFhirClient.CreateAsync(new Patient { Meta = meta, Gender = AdministrativeGender.Male, Name = new List<HumanName> { new HumanName { Given = new[] { SmithPatientGivenName }, Family = "Smith" } }, ManagingOrganization = new ResourceReference($"Organization/{organization.Id}") })).Resource;
                TrumanPatient = (await TestFhirClient.CreateAsync(new Patient { Meta = meta, Gender = AdministrativeGender.Male, Name = new List<HumanName> { new HumanName { Given = new[] { TrumanPatientGivenName }, Family = "Truman" } } })).Resource;

                DeviceLoincSubject = (await TestFhirClient.CreateAsync(new Device { Meta = meta })).Resource;
                DeviceSnomedSubject = (await TestFhirClient.CreateAsync(new Device { Meta = meta })).Resource;

                var adamsLoincObservation = await CreateObservation(AdamsPatient, loincCode);
                var smithLoincObservation = await CreateObservation(SmithPatient, loincCode);
                var smithSnomedObservation = await CreateObservation(SmithPatient, snomedCode);
                var trumanLoincObservation = await CreateObservation(TrumanPatient, loincCode);
                var trumanSnomedObservation = await CreateObservation(TrumanPatient, snomedCode);
                var deviceLoincObservation = await CreateObservation(DeviceLoincSubject, loincCode);
                var deviceSnomedObservation = await CreateObservation(DeviceSnomedSubject, snomedCode);

                SmithSnomedDiagnosticReport = await CreateDiagnosticReport(SmithPatient, smithSnomedObservation, snomedCode);
                TrumanSnomedDiagnosticReport = await CreateDiagnosticReport(TrumanPatient, trumanSnomedObservation, snomedCode);
                SmithLoincDiagnosticReport = await CreateDiagnosticReport(SmithPatient, smithLoincObservation, loincCode);
                TrumanLoincDiagnosticReport = await CreateDiagnosticReport(TrumanPatient, trumanLoincObservation, loincCode);

                var deletedPatient = (await TestFhirClient.CreateAsync(new Patient { Meta = meta, Gender = AdministrativeGender.Male, Name = new List<HumanName> { new HumanName { Given = new[] { "Delete" }, Family = "Delete" } } })).Resource;
                await TestFhirClient.CreateAsync(new CareTeam() { Meta = meta, Subject = new ResourceReference($"Patient/{AdamsPatient.Id}") });
                await TestFhirClient.CreateAsync(new CareTeam() { Meta = meta, Subject = new ResourceReference($"Patient/{deletedPatient.Id}") });

                await TestFhirClient.DeleteAsync(deletedPatient);

                var group = new Group
                {
                    Meta = new Meta { Tag = new List<Coding> { new Coding("testTag", Tag) } },
                    Type = Group.GroupType.Person,
#if Stu3 || R4 || R4B
                    Actual = true,
#else
                    Active = true,
                    Membership = Group.GroupMembershipBasis.Definitional,
#endif
                    Member = new List<Group.MemberComponent>
                    {
                        new Group.MemberComponent { Entity = new ResourceReference($"Patient/{AdamsPatient.Id}") },
                        new Group.MemberComponent { Entity = new ResourceReference($"Patient/{SmithPatient.Id}") },
                        new Group.MemberComponent { Entity = new ResourceReference($"Patient/{TrumanPatient.Id}") },
                    },
                };

                PatientGroup = (await TestFhirClient.CreateAsync(group)).Resource;

                async Task<DiagnosticReport> CreateDiagnosticReport(Patient patient, Observation observation, CodeableConcept code)
                {
                    return (await TestFhirClient.CreateAsync(
                        new DiagnosticReport
                        {
                            Meta = meta,
                            Status = DiagnosticReport.DiagnosticReportStatus.Final,
                            Code = code,
                            Subject = new ResourceReference($"Patient/{patient.Id}"),
                            Result = new List<ResourceReference> { new ResourceReference($"Observation/{observation.Id}") },
                        })).Resource;
                }

                async Task<Observation> CreateObservation<T>(T subject, CodeableConcept code)
                    where T : DomainResource
                {
                    return (await TestFhirClient.CreateAsync(
                        new Observation()
                        {
                            Meta = meta,
                            Status = ObservationStatus.Final,
                            Code = code,
                            Subject = new ResourceReference($"{subject.TypeName}/{subject.Id}"),
                        })).Resource;
                }
            }
        }
    }
}
