<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Bucket;

use Google\ApiCore\ApiException;
use Google\ApiCore\CredentialsWrapper;
use Google\Auth\Credentials\ServiceAccountCredentials;
use Google\Auth\Credentials\ServiceAccountJwtAccessCredentials;
use Google\Auth\CredentialsLoader;
use Google\Cloud\BigQuery\AnalyticsHub\V1\AnalyticsHubServiceClient;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DataExchange;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDataset;
use Google\Cloud\BigQuery\AnalyticsHub\V1\DestinationDatasetReference;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Gapic\AnalyticsHubServiceGapicClient;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing;
use Google\Cloud\BigQuery\AnalyticsHub\V1\Listing\BigQueryDatasetSource;
use Google\Cloud\Core\Exception\BadRequestException;
use Google\Cloud\Iam\V1\Binding;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\CredentialsHelper;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Create\GrantBucketAccessToReadOnlyRoleHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Link\LinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\Share\ShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnLink\UnLinkBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Bucket\UnShare\UnShareBucketHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\NameGenerator;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleCommand;
use Keboola\StorageDriver\Command\Bucket\GrantBucketAccessToReadOnlyRoleResponse;
use Keboola\StorageDriver\Command\Bucket\LinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\LinkedBucketResponse;
use Keboola\StorageDriver\Command\Bucket\ShareBucketCommand;
use Keboola\StorageDriver\Command\Bucket\ShareBucketResponse;
use Keboola\StorageDriver\Command\Bucket\UnlinkBucketCommand;
use Keboola\StorageDriver\Command\Bucket\UnshareBucketCommand;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Throwable;

class ShareLinkBucketTest extends BaseCase
{
    private const TESTTABLE_BEFORE_NAME = 'TESTTABLE_BEFORE';
    private const TESTTABLE_AFTER_NAME = 'TESTTABLE_AFTER';

    protected GenericBackendCredentials $sourceProjectCredentials;

    protected CreateProjectResponse $sourceProjectResponse;

    protected GenericBackendCredentials $targetProjectCredentials;

    protected CreateProjectResponse $targetProjectResponse;

    protected function setUp(): void
    {
        parent::setUp();
        // project1 shares bucket
        $this->sourceProjectCredentials = $this->projects[0][0];
        $this->sourceProjectResponse = $this->projects[0][1];

        // project2 checks the access
        $this->targetProjectCredentials = $this->projects[1][0];
        $this->targetProjectResponse = $this->projects[1][1];

        $this->externalProjectCredentials = $this->projects[2][0];
        $this->externalProjectResponse = $this->projects[2][1];

        $bucketResponse = $this->createTestBucket($this->projects[2][0], $this->projects[2][2]);
        $this->bucketResponse = $bucketResponse;
    }

    public function testShareAndLinkExtBucket(): void
    {
        // prepare test external table
        $externalBucketName = $this->bucketResponse->getCreateBucketObjectName();
        $externalTableName = md5($this->getName()) . '_Test_table';
        $this->prepareTestTable($externalBucketName, $externalTableName);

        // this part simulate user who want to register ext bucket
        // 1. and 2. will be done in one step, but we need to test it can't be registered before grant permission
        // 1. User which want to register external bucket create exchanged and listing
        $externalAnalyticHubClient = $this->clientManager->getAnalyticHubClient($this->externalProjectCredentials);
        [$dataExchange, $createdListing] = $this->prepareExternalBucketForRegistration(
            $externalAnalyticHubClient,
            $externalBucketName,
        );

        $this->grantMainProjectToRegisterExternalBucket($externalAnalyticHubClient, $dataExchange);

        $parsedName = AnalyticsHubServiceClient::parseName($createdListing->getName());

        $handler = new GrantBucketAccessToReadOnlyRoleHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new GrantBucketAccessToReadOnlyRoleCommand())
            ->setPath([
                $parsedName['project'],
                $parsedName['location'],
                $parsedName['data_exchange'],
                $parsedName['listing'],
            ])
            ->setDestinationObjectName('test_external')
            ->setBranchId('123')
            ->setStackPrefix($this->getStackPrefix());
        $meta = new Any();
        $meta->pack(
            (new GrantBucketAccessToReadOnlyRoleCommand\GrantBucketAccessToReadOnlyRoleBigqueryMeta()),
        );
        $command->setMeta($meta);

        /** @var GrantBucketAccessToReadOnlyRoleResponse $result */
        $result = $handler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertSame('123_test_external', $result->getCreateBucketObjectName());
        // Validate is bucket added
        $mainBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->sourceProjectCredentials);
        $registeredExternalBucketInMainProject = $mainBqClient->dataset('123_test_external');
        $registeredTables = $registeredExternalBucketInMainProject->tables();
        $this->assertCount(1, $registeredTables);

        // external bucket prepare
        // is registerd to source project


        // ----------------------------------------
        // time to share it

        $baseCredentials = CredentialsHelper::getCredentialsArray($this->getCredentials());
        $sourceClientEmail = json_decode($this->sourceProjectCredentials->getPrincipal())->client_email;

        // Set up base credentials using the JSON key array
        $baseCredentials = new ServiceAccountCredentials(
            ['https://www.googleapis.com/auth/cloud-platform'],
            $baseCredentials
        );

        $token = $baseCredentials->fetchAuthToken()['access_token'];

        sleep(10);
        // Use the IAM Credentials API to generate an impersonated access token for `sourceClientEmail`
        $httpClient = new Client();
        $impersonationUrl = 'https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/' . $sourceClientEmail . ':generateAccessToken';
        $response = $httpClient->post($impersonationUrl, [
            'headers' => [
                'Authorization' => 'Bearer ' . $token,
                'Content-Type' => 'application/json',
            ],
            'json' => [
                'scope' => ['https://www.googleapis.com/auth/cloud-platform'],
            ],
        ]);

        // Extract the impersonated token
        // Fetch the response body as a string
        $responseBody = $response->getBody()->getContents();
        // Decode the JSON response
        $impersonatedTokenData = json_decode($responseBody, true);
        // Access the impersonated access token
        $impersonatedToken = $impersonatedTokenData['accessToken'];

        $credentials = new AccessTokenCredentials($impersonatedToken);

        $externalAnalyticHubClient = new AnalyticsHubServiceClient([
            'credentials' => $credentials,
        ]);
        $publicPart = (array) json_decode(
            $this->targetProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        $iamExchangerPolicy = $externalAnalyticHubClient->getIamPolicy($dataExchange->getName());
        $binding = $iamExchangerPolicy->getBindings();
        $binding[] = new Binding([
            'role' => 'roles/analyticshub.subscriber',
            'members' => ['serviceAccount:' . $publicPart['client_email']],
        ]);
        $iamExchangerPolicy->setBindings($binding);
        $externalAnalyticHubClient->setIamPolicy($dataExchange->getName(), $iamExchangerPolicy);
        // ----------------------------------------
        // time to link pico


        $baseCredentials = CredentialsHelper::getCredentialsArray($this->getCredentials());
        $destClientEmail = json_decode($this->targetProjectCredentials->getPrincipal())->client_email;

        // Set up base credentials using the JSON key array
        $baseCredentials = new ServiceAccountCredentials(
            ['https://www.googleapis.com/auth/cloud-platform'],
            $baseCredentials
        );

        $token = $baseCredentials->fetchAuthToken()['access_token'];

        sleep(10);
        // Use the IAM Credentials API to generate an impersonated access token for `sourceClientEmail`
        $httpClient = new Client();
        $impersonationUrl = 'https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/' . $destClientEmail . ':generateAccessToken';
        $response = $httpClient->post($impersonationUrl, [
            'headers' => [
                'Authorization' => 'Bearer ' . $token,
                'Content-Type' => 'application/json',
            ],
            'json' => [
                'scope' => ['https://www.googleapis.com/auth/cloud-platform'],
            ],
        ]);

        // Extract the impersonated token
        // Fetch the response body as a string
        $responseBody = $response->getBody()->getContents();
        // Decode the JSON response
        $impersonatedTokenData = json_decode($responseBody, true);
        // Access the impersonated access token
        $impersonatedToken = $impersonatedTokenData['accessToken'];

        $credentials = new AccessTokenCredentials($impersonatedToken);

        $destAnalyticHubClient = new AnalyticsHubServiceClient([
            'credentials' => $credentials,
        ]);

        $nameGenerator = new NameGenerator($command->getStackPrefix());

        $newBucketDatabaseName = $nameGenerator->createObjectNameForBucketInProject(
            'test',
            null,
        );

        $publicPart = (array) json_decode(
            $this->targetProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $sourceProjectId */
        $targetProjectId = $publicPart['project_id'];

        $datasetReference = new DestinationDatasetReference();
        $datasetReference->setProjectId($targetProjectId);
        $datasetReference->setDatasetId($newBucketDatabaseName);

        $destinationDataset = new DestinationDataset([
            'dataset_reference' => $datasetReference,
            'location' => 'us',//$credentialsMeta->getRegion(),
        ]);
        $destAnalyticHubClient->subscribeListing($createdListing->getName(), [
            'destinationDataset' => $destinationDataset,
        ]);


        $targetProjectBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );
        $targetDataset = $targetProjectBqClient->dataset('test');
        $this->assertTrue($targetDataset->exists());
        $testTableBefore = $targetDataset->table($externalTableName);
        $this->assertTrue($testTableBefore->exists());
        $dataBefore = iterator_to_array($testTableBefore->rows());
        $this->assertCount(3, $dataBefore);
        ///

    }

    private function grantMainProjectToRegisterExternalBucket(
        AnalyticsHubServiceClient $externalAnalyticHubClient,
        DataExchange $dataExchange,
    ): void {
        $mainCredentials = CredentialsHelper::getCredentialsArray($this->sourceProjectCredentials);
        $iamExchangerPolicy = $externalAnalyticHubClient->getIamPolicy($dataExchange->getName());
        $binding = $iamExchangerPolicy->getBindings();
        $binding[] = new Binding([
            'role' => 'roles/analyticshub.admin',
            'members' => ['serviceAccount:' . $mainCredentials['client_email']],
        ]);
        $iamExchangerPolicy->setBindings($binding);
        $externalAnalyticHubClient->setIamPolicy($dataExchange->getName(), $iamExchangerPolicy);
    }

    /**
     * @return array{DataExchange, Listing}
     */
    private function prepareExternalBucketForRegistration(
        AnalyticsHubServiceClient $externalAnalyticHubClient,
        string $bucketDatabaseName,
        string $location = BaseCase::DEFAULT_LOCATION,
    ): array {
        $externalCredentials = CredentialsHelper::getCredentialsArray($this->externalProjectCredentials);
        $externalProjectStringId = $externalCredentials['project_id'];

        $dataExchangeId = str_replace('-', '_', $externalProjectStringId) . '_external';
        $formattedParent = $externalAnalyticHubClient->locationName($externalProjectStringId, $location);

        // 1.1 Create exchanger in source project
        $dataExchange = new DataExchange();
        $dataExchange->setDisplayName($dataExchangeId);

        try {
            // delete if exist in case of retry
            $dataExchangeName = AnalyticsHubServiceClient::dataExchangeName(
                $externalProjectStringId,
                $location,
                $dataExchangeId,
            );
            $dataExchange = $externalAnalyticHubClient->getDataExchange($dataExchangeName);
            $externalAnalyticHubClient->deleteDataExchange($dataExchange->getName());
        } catch (Throwable $e) {
            // ignore
        }

        $dataExchange = $externalAnalyticHubClient->createDataExchange(
            $formattedParent,
            $dataExchangeId,
            $dataExchange,
        );

        $listingId = str_replace('-', '_', $externalCredentials['project_id']) . '_listing';
        $lst = new BigQueryDatasetSource([
            'dataset' => sprintf(
                'projects/%s/datasets/%s',
                $externalProjectStringId,
                $bucketDatabaseName,
            ),
        ]);
        $listing = new Listing();
        $listing->setBigqueryDataset($lst);
        $listing->setDisplayName($listingId);

        // 1.2 Create listing for extern bucket
        $createdListing = $externalAnalyticHubClient->createListing($dataExchange->getName(), $listingId, $listing);
        return [$dataExchange, $createdListing];
    }

    private function prepareTestTable(string $bucketDatabaseName, string $externalTableName): void
    {
        $this->createTestTable($this->externalProjectCredentials, $bucketDatabaseName, $externalTableName);

        // FILL DATA
        $insertGroups = [
            [
                'columns' => '`id`, `name`, `large`',
                'rows' => [
                    "1, 'external', 'data'",
                    "2, 'data from', 'external table'",
                    "3, 'it works', 'awesome !'",
                ],
            ],
        ];
        $this->fillTableWithData(
            $this->externalProjectCredentials,
            $bucketDatabaseName,
            $externalTableName,
            $insertGroups,
        );
    }

    public function testShareAndLinkBucket(): void
    {
        $bucketResponse = $this->createTestBucket($this->sourceProjectCredentials, $this->projects[0][2]);

        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();

        $sourceBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->sourceProjectCredentials);
        $linkedBucketSchemaName = $bucketDatabaseName . '_LINKED';

        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('ID')
            ->setType(Bigquery::TYPE_INTEGER);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName(self::TESTTABLE_BEFORE_NAME)
            ->setColumns($columns);
        $handler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`) VALUES (1)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier(self::TESTTABLE_BEFORE_NAME),
        )));

        $targetProjectBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );

//      check that the Project2 cannot access the table yet
        $dataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertFalse($dataset->exists());

        $publicPart = (array) json_decode(
            $this->sourceProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $publicPart['project_id'];
        // share the bucket
        $handler = new ShareBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceBucketId('123456')
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $meta = new Any();
        $meta->pack((new ShareBucketCommand\ShareBucketBigqueryCommandMeta()));
        $command->setMeta($meta);
        $result = $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(ShareBucketResponse::class, $result);
        $listing = $result->getBucketShareRoleName();
        $this->assertNotEmpty($listing);
        $publicPart = (array) json_decode(
            $this->targetProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $targetProjectId */
        $targetProjectId = $publicPart['project_id'];
        // link the bucket
        $handler = new LinkBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($targetProjectId)
            ->setTargetBucketId($linkedBucketSchemaName)
            ->setSourceShareRoleName($listing); // listing

        $meta = new Any();
        $meta->pack(new LinkBucketCommand\LinkBucketBigqueryMeta());
        $command->setMeta($meta);
        // root credentials and creating grants
        $result = $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(LinkedBucketResponse::class, $result);
        $linkedBucketSchemaName = $result->getLinkedBucketObjectName();
        $this->assertNotEmpty($linkedBucketSchemaName);
        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared())
            ->setName('ID')
            ->setType(Bigquery::TYPE_INTEGER);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName(self::TESTTABLE_AFTER_NAME)
            ->setColumns($columns);
        $handler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        // check that there is no need to re-share or whatever
        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`) VALUES (1)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier(self::TESTTABLE_AFTER_NAME),
        )));

        $targetDataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());
        $testTableBefore = $targetDataset->table(self::TESTTABLE_BEFORE_NAME);
        $this->assertTrue($testTableBefore->exists());
        $dataBefore = iterator_to_array($testTableBefore->rows());

        $testTableAfter = $targetDataset->table(self::TESTTABLE_AFTER_NAME);
        $this->assertTrue($testTableAfter->exists());
        $dataAfter = iterator_to_array($testTableAfter->rows());

        $this->assertEquals([['ID' => '1']], $dataAfter);
        $this->assertEquals($dataBefore, $dataAfter);

        // unlink and check that target project cannot access it anymore
        $unlinkHandler = new UnLinkBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new UnLinkBucketCommand())
            ->setBucketObjectName($linkedBucketSchemaName);

        $unlinkHandler(
            $this->targetProjectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        // check that the Project2 cannot access the table anymore
        $targetDataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertFalse($targetDataset->exists());
    }

    public function testShareUnshare(): void
    {
        $bucketResponse = $this->createTestBucket($this->sourceProjectCredentials, $this->projects[0][2]);

        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();
        $publicPart = (array) json_decode(
            $this->sourceProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $publicPart['project_id'];
        $handler = new ShareBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceBucketId('1234')
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $meta = new Any();
        $meta->pack((new ShareBucketCommand\ShareBucketBigqueryCommandMeta()));
        $command->setMeta($meta);
        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($this->getCredentials());

        $formattedName = $analyticHubClient::listingName(
            $sourceProjectId,
            BaseCase::DEFAULT_LOCATION,
            $this->sourceProjectResponse->getProjectReadOnlyRoleName(),
            '1234',
        );
        $listing = $analyticHubClient->getListing($formattedName);
        $this->assertNotNull($listing->getName());

        $handler = new UnShareBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new UnShareBucketCommand())
            ->setBucketShareRoleName($listing->getName());

        $handler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        try {
            $analyticHubClient->getListing($formattedName);
            $this->fail('Should fail!');
        } catch (ApiException $e) {
            $this->assertSame('NOT_FOUND', $e->getStatus());
        }
    }

    public function testShareUnshareLinkedBucket(): void
    {
        $bucketResponse = $this->createTestBucket($this->sourceProjectCredentials, $this->projects[0][2]);

        $bucketDatabaseName = $bucketResponse->getCreateBucketObjectName();

        $publicPart = (array) json_decode(
            $this->sourceProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $sourceProjectId */
        $sourceProjectId = $publicPart['project_id'];
        $handler = new ShareBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new ShareBucketCommand())
            ->setSourceProjectId($sourceProjectId)
            ->setSourceBucketObjectName($bucketDatabaseName)
            ->setSourceBucketId('12345')
            ->setSourceProjectReadOnlyRoleName($this->sourceProjectResponse->getProjectReadOnlyRoleName());

        $meta = new Any();
        $meta->pack(new ShareBucketCommand\ShareBucketBigqueryCommandMeta());
        $command->setMeta($meta);
        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $sourceBqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->sourceProjectCredentials);

        $handler = new CreateTableHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatabaseName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared())
            ->setName('ID')
            ->setType(Bigquery::TYPE_INTEGER);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName(self::TESTTABLE_AFTER_NAME)
            ->setColumns($columns);
        $handler(
            $this->sourceProjectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
        // check that there is no need to re-share or whatever
        $sourceBqClient->runQuery($sourceBqClient->query(sprintf(
            'INSERT INTO %s.%s (`ID`) VALUES (1)',
            BigqueryQuote::quoteSingleIdentifier($bucketDatabaseName),
            BigqueryQuote::quoteSingleIdentifier(self::TESTTABLE_AFTER_NAME),
        )));

        $analyticHubClient = $this->clientManager->getAnalyticHubClient($this->getCredentials());

        $formattedName = $analyticHubClient::listingName(
            $sourceProjectId,
            BaseCase::DEFAULT_LOCATION,
            $this->sourceProjectResponse->getProjectReadOnlyRoleName(),
            '12345',
        );
        $listing = $analyticHubClient->getListing($formattedName);
        $this->assertNotNull($listing->getName());

        $publicPart = (array) json_decode(
            $this->targetProjectResponse->getProjectUserName(),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        /** @var string $targetProjectId */
        $targetProjectId = $publicPart['project_id'];
        $linkedBucketSchemaName = $bucketDatabaseName . '_LINKED';

        $handler = new LinkBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new LinkBucketCommand())
            ->setStackPrefix($this->getStackPrefix())
            ->setTargetProjectId($targetProjectId)
            ->setTargetBucketId($linkedBucketSchemaName)
            ->setSourceShareRoleName($listing->getName()); // listing

        $meta = new Any();
        $meta->pack(new LinkBucketCommand\LinkBucketBigqueryMeta());
        $command->setMeta($meta);
        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $targetProjectBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );
        $targetDataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());
        $testTableBefore = $targetDataset->table(self::TESTTABLE_AFTER_NAME);
        $this->assertTrue($testTableBefore->exists());

        $handler = new UnShareBucketHandler($this->clientManager);
        $handler->setInternalLogger($this->log);
        $command = (new UnShareBucketCommand())
            ->setBucketShareRoleName($listing->getName());

        $handler(
            $this->getCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $targetProjectBqClient = $this->clientManager->getBigQueryClient(
            $this->testRunId,
            $this->targetProjectCredentials,
        );
        $targetDataset = $targetProjectBqClient->dataset($linkedBucketSchemaName);
        $this->assertTrue($targetDataset->exists());
        $testTableBefore = $targetDataset->table(self::TESTTABLE_AFTER_NAME);

        // after unshare the table is not available
        // in connection you can't just unshare a bucket that is lined up first so this is an edge case
        // handled in connection
        $this->expectException(BadRequestException::class);
        $testTableBefore->exists();
    }
}
