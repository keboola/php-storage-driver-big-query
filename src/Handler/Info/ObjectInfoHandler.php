<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\BigQuery\Handler\Info;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\BigQuery\Table;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\BigQuery\GCPClientManager;
use Keboola\StorageDriver\BigQuery\Handler\BaseHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\TableReflectionResponseTransformer;
use Keboola\StorageDriver\BigQuery\Handler\Table\ViewReflectionResponseTransformer;
use Keboola\StorageDriver\Command\Info\DatabaseInfo;
use Keboola\StorageDriver\Command\Info\ObjectInfo;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\SchemaInfo;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\ObjectNotFoundException;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\UnknownObjectException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use Throwable;

final class ObjectInfoHandler extends BaseHandler
{
    public GCPClientManager $clientManager;

    public function __construct(GCPClientManager $clientManager)
    {
        parent::__construct();
        $this->clientManager = $clientManager;
    }

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof ObjectInfoCommand);

        assert($runtimeOptions->getMeta() === null);

        $bqClient = $this->clientManager->getBigQueryClient($runtimeOptions->getRunId(), $credentials);

        $path = ProtobufHelper::repeatedStringToArray($command->getPath());

        assert(count($path) !== 0, 'Error empty path.');

        $response = (new ObjectInfoResponse())
            ->setPath($command->getPath())
            ->setObjectType($command->getExpectedObjectType());

        switch ($command->getExpectedObjectType()) {
            // DATABASE === PROJECT in BQ
            case ObjectType::DATABASE:
                return $this->getDatabaseResponse($path, $bqClient, $response);
            case ObjectType::SCHEMA:
                return $this->getSchemaResponse($path, $bqClient, $response);
            case ObjectType::VIEW:
                return $this->getViewResponse($path, $bqClient, $response);
            case ObjectType::TABLE:
                return $this->getTableResponse($path, $response, $bqClient);
            default:
                throw new UnknownObjectException(ObjectType::name($command->getExpectedObjectType()));
        }
    }

    /**
     * @return Generator<int, ObjectInfo>
     */
    private function getChildSchemas(BigQueryClient $bqClient): Generator
    {
        $datasets = $bqClient->datasets();
        /** @var Dataset $child */
        foreach ($datasets as $child) {
            yield (new ObjectInfo())
                ->setObjectType(ObjectType::SCHEMA)
                ->setObjectName($child->info()['datasetReference']['datasetId']);
        }
    }

    /**
     * @param string[] $path
     */
    private function getDatabaseResponse(
        array $path,
        BigQueryClient $bqClient,
        ObjectInfoResponse $response
    ): ObjectInfoResponse {
        assert(count($path) === 1, 'Error path must have exactly one element.');
        $objects = new RepeatedField(GPBType::MESSAGE, ObjectInfo::class);
        foreach ($this->getChildSchemas($bqClient) as $object) {
            $objects[] = $object;
        }

        $infoObject = new DatabaseInfo();
        $infoObject->setObjects($objects);

        $response->setDatabaseInfo($infoObject);

        return $response;
    }

    /**
     * @param string[] $path
     */
    private function getSchemaResponse(
        array $path,
        BigQueryClient $bqClient,
        ObjectInfoResponse $response
    ): ObjectInfoResponse {
        assert(count($path) === 1, 'Error path must have exactly one element.');
        $infoObject = new SchemaInfo();
        $objects = new RepeatedField(GPBType::MESSAGE, ObjectInfo::class);
        $dataset = $this->getDataset($bqClient, $path[0]);
        foreach ($this->getObjectsForDataset($bqClient, $dataset) as $object) {
            $objects[] = $object;
        }
        $infoObject->setObjects($objects);
        $response->setSchemaInfo($infoObject);
        return $response;
    }

    /**
     * @return Generator<int, ObjectInfo>
     */
    private function getObjectsForDataset(BigQueryClient $client, Dataset $dataset): Generator
    {
        //TABLE: A normal BigQuery table.
        //VIEW: A virtual table defined by a SQL query.
        //EXTERNAL: A table that references data stored in an external storage system, such as Google Cloud Storage.
        //MATERIALIZED_VIEW: A precomputed view defined by a SQL query.
        //SNAPSHOT: An immutable BigQuery table that preserves the contents of a base table at a particular time.
        /** @var Table $table */
        foreach ($dataset->tables() as $table) {
            $this->internalLogger->debug(sprintf(
                'Processing table "%s".',
                $table->id()
            ));
            $info = $table->info();
            try {
                // we need to run select as query as ->rows method is not supported for view
                $client->runQuery($client->query(sprintf(
                    'SELECT * FROM %s.%s.%s LIMIT 1',
                    BigqueryQuote::quoteSingleIdentifier($info['tableReference']['projectId']),
                    BigqueryQuote::quoteSingleIdentifier($info['tableReference']['datasetId']),
                    BigqueryQuote::quoteSingleIdentifier($info['tableReference']['tableId']),
                )));
            } catch (Throwable $e) {
                $this->userLogger->warning(sprintf(
                    'Selecting data from table "%s" failed with error: "%s" Table was ignored',
                    $info['id'],
                    $e->getMessage()
                ), [
                    'info' => $info,
                ]);
                continue;
            }
            if ($info['type'] === 'VIEW' || $info['type'] === 'MATERIALIZED_VIEW') {
                $this->internalLogger->debug(sprintf(
                    'Found view "%s".',
                    $table->id()
                ));
                yield (new ObjectInfo())
                    ->setObjectType(ObjectType::VIEW)
                    ->setObjectName($table->id());
                continue;
            }
            $this->internalLogger->debug(sprintf(
                'Found table "%s".',
                $table->id()
            ));
            // TABLE,EXTERNAL,SNAPSHOT
            yield (new ObjectInfo())
                ->setObjectType(ObjectType::TABLE)
                ->setObjectName($table->id());
        }
    }

    /**
     * @param string[] $path
     */
    private function getTableResponse(
        array $path,
        ObjectInfoResponse $response,
        BigQueryClient $bqClient
    ): ObjectInfoResponse {
        assert(count($path) === 2, 'Error path must have exactly two elements.');
        $this->getDataset($bqClient, $path[0]);
        try {
            $response->setTableInfo(TableReflectionResponseTransformer::transformTableReflectionToResponse(
                $path[0],
                new BigqueryTableReflection(
                    $bqClient,
                    $path[0],
                    $path[1]
                )
            ));
        } catch (TableNotExistsReflectionException $e) {
            throw new ObjectNotFoundException($path[1]);
        }
        return $response;
    }

    /**
     * @param string[] $path
     */
    private function getViewResponse(
        array $path,
        BigQueryClient $bqClient,
        ObjectInfoResponse $response
    ): ObjectInfoResponse {
        assert(count($path) === 2, 'Error path must have exactly two elements.');
        $this->getDataset($bqClient, $path[0]);

        $response->setViewInfo(ViewReflectionResponseTransformer::transformTableReflectionToResponse(
            $path[0],
            new BigqueryTableReflection(
                $bqClient,
                $path[0],
                $path[1]
            )
        ));
        return $response;
    }

    private function getDataset(BigQueryClient $bqClient, string $datasetName): Dataset
    {
        $dataset = $bqClient->dataset($datasetName);
        if ($dataset->exists() === false) {
            throw new ObjectNotFoundException($datasetName);
        }

        return $dataset;
    }
}
