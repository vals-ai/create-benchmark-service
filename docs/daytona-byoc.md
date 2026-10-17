# Daytona BYOC

Use the `daytona-byoc` provider type to require a Daytona custom region:

```json
{
  "type": "daytona-byoc",
  "DAYTONA_API_KEY": "<organization API key>",
  "DAYTONA_API_URL": "https://app.daytona.io/api",
  "DAYTONA_TARGET": "<canonical custom region ID>",
  "DAYTONA_ORGANIZATION_ID": "<organization ID>"
}
```

The provider verifies that the region is custom and belongs to the configured
organization before sandbox operations. The API key needs region read access
in addition to the sandbox permissions used by the workload. Successful
verification is cached for the provider instance; failed verification stops
the operation. Daytona's hosted management API remains a dependency.

For the Vals `/v1` grading service, set `GRADING_SANDBOX_PROVIDER=daytona-byoc`
and the four `DAYTONA_*` environment variables shown above.

The provider rejects snapshots that explicitly select another region and
checks actual sandbox placement before use, recovery, or deletion. It never
automatically deletes a sandbox found outside the configured region, including
one returned by a create request. The error includes that sandbox's ID for
operator investigation. Capacity reporting includes only the configured
region. The existing organization-wide admission pool is preserved.

The `daytona-byoc` discriminator survives setup and evaluation request
serialization. Benchmark services that create extra grading sandboxes must
use the request's provider. Deploy this framework version in callers and all
participating benchmark services before submitting BYOC work. Old framework
versions reject the new type. Never change it to `daytona` to bypass that error.

The existing `daytona` type remains the default and keeps its current behavior.
There is no automatic fallback from BYOC. Change the selected provider or its
region only after all affected work has finished. Images and snapshots must
already be available in the selected region; this feature does not copy them.
