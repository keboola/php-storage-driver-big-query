<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery;

class GCPServiceIds
{
    public const SERVICE_USAGE_SERVICE = 'serviceusage.googleapis.com';
    public const IAM_SERVICE = 'iam.googleapis.com';
    public const IAM_CREDENTIALS_SERVICE = 'iamcredentials.googleapis.com';
    public const BIGQUERY_SERVICE = 'bigquery.googleapis.com';
    public const BIGQUERY_MIGRATION_SERVICE = 'bigquerymigration.googleapis.com';
    public const BIGQUERY_STORAGE_SERVICE = 'bigquerystorage.googleapis.com';
}
