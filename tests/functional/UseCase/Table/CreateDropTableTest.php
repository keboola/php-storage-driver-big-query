<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\Backend\BigQuery\Clustering;
use Keboola\StorageDriver\Backend\BigQuery\RangePartitioning;
use Keboola\StorageDriver\Backend\BigQuery\TimePartitioning;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\BadTableDefinitionException;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Command\Table\TableColumnShared;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;

class CreateDropTableTest extends BaseCase
{
    protected CreateBucketResponse $bucketResponse;

    private GenericBackendCredentials $projectCredentials;

    protected function setUp(): void
    {
        parent::setUp();
        $this->projectCredentials = $this->projects[0][0];

        $this->bucketResponse = $this->createTestBucket($this->projects[0][0], $this->projects[0][2]);
    }

    public function testCreateTable(): void
    {
        $tableName = $this->getTestHash() . '_Test_table';
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = (new TableColumnShared)
            ->setName('name')
            ->setType(Bigquery::TYPE_STRING)
            ->setLength('50')
            ->setNullable(true)
            ->setDefault("'Some Default'");
        $columns[] = (new TableColumnShared)
            ->setName('large')
            ->setType(Bigquery::TYPE_BIGNUMERIC)
            ->setLength('76,38')
            ->setDefault('185.554');
        $columns[] = (new TableColumnShared)
            ->setName('ordes')
            ->setType(Bigquery::TYPE_ARRAY)
            ->setLength('STRUCT<x ARRAY<STRUCT<xz ARRAY<INT64>>>>');
        $columns[] = (new TableColumnShared)
            ->setName('organization')
            ->setType(Bigquery::TYPE_STRUCT)
            ->setLength('x ARRAY<INT64>');
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns);
        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertNotNull($response->getTableInfo());

        $columns = $response->getTableInfo()->getColumns();
        $this->assertCount(5, $columns);

        // check column ID
        /** @var TableInfo\TableColumn $column */
        $column = $columns[0];
        $this->assertSame('id', $column->getName());
        $this->assertSame(Bigquery::TYPE_INTEGER, $column->getType());
        $this->assertFalse($column->getNullable());
        $this->assertSame('', $column->getDefault());

        // check column NAME
        /** @var TableInfo\TableColumn $column */
        $column = $columns[1];
        $this->assertSame('name', $column->getName());
        $this->assertSame(Bigquery::TYPE_STRING, $column->getType());
        $this->assertSame('50', $column->getLength());
        $this->assertTrue($column->getNullable());
        $this->assertSame("'Some Default'", $column->getDefault());

        // check column LARGE
        /** @var TableInfo\TableColumn $column */
        $column = $columns[2];
        $this->assertSame('large', $column->getName());
        $this->assertSame(Bigquery::TYPE_BIGNUMERIC, $column->getType());
        $this->assertSame('76,38', $column->getLength());
        $this->assertFalse($column->getNullable());
        $this->assertSame('185.554', $column->getDefault());

        // check column array
        /** @var TableInfo\TableColumn $column */
        $column = $columns[3];
        $this->assertSame('ordes', $column->getName());
        $this->assertSame(Bigquery::TYPE_ARRAY, $column->getType());
        $this->assertSame('STRUCT<x ARRAY<STRUCT<xz ARRAY<INTEGER>>>>', $column->getLength());
        $this->assertTrue($column->getNullable());

        // check column array
        /** @var TableInfo\TableColumn $column */
        $column = $columns[4];
        $this->assertSame('organization', $column->getName());
        $this->assertSame(Bigquery::TYPE_STRUCT, $column->getType());
        $this->assertSame('x ARRAY<INTEGER>', $column->getLength());
        $this->assertTrue($column->getNullable());

        // DROP TABLE
        $handler = new DropTableHandler($this->clientManager);
        $command = (new DropTableCommand())
            ->setPath($path)
            ->setTableName($tableName);

        $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $bqClient = $this->clientManager->getBigQueryClient($this->testRunId, $this->projectCredentials);

        $bucket = $bqClient->dataset($bucketDatasetName);
        $this->assertTrue($bucket->exists());

        $table = $bucket->table($tableName);
        $this->assertFalse($table->exists());
    }

    public function testCreateTableWithPartitioningClustering(): void
    {
        // Range partitioning and clustering
        $tableInfo = $this->createTableForPartitioning(
            (new CreateTableCommand\BigQueryTableMeta())
                ->setClustering((new Clustering())->setFields(['id']))
                ->setRangePartitioning((new RangePartitioning())
                    ->setField('id')
                    ->setRange((new RangePartitioning\Range())
                        ->setStart('0')
                        ->setEnd('10')
                        ->setInterval('1'))),
            'range'
        );

        $this->assertNotNull($tableInfo->getMeta());
        $meta = $tableInfo->getMeta()->unpack();
        $this->assertInstanceOf(TableInfo\BigQueryTableMeta::class, $meta);
        $this->assertNotNull($meta->getClustering());
        $this->assertSame(['id'], ProtobufHelper::repeatedStringToArray($meta->getClustering()->getFields()));
        $this->assertNull($meta->getTimePartitioning());
        $this->assertNotNull($meta->getRangePartitioning());
        $this->assertSame('id', $meta->getRangePartitioning()->getField());
        $this->assertNotNull($meta->getRangePartitioning()->getRange());
        $this->assertSame('0', $meta->getRangePartitioning()->getRange()->getStart());
        $this->assertSame('10', $meta->getRangePartitioning()->getRange()->getEnd());
        $this->assertSame('1', $meta->getRangePartitioning()->getRange()->getInterval());

        // Range partitioning and clustering
        $expirationMs = (string) (1000 * 60 * 60 * 24 * 10);
        $tableInfo = $this->createTableForPartitioning(
            (new CreateTableCommand\BigQueryTableMeta())
                ->setClustering((new Clustering())->setFields(['id']))
                ->setTimePartitioning((new TimePartitioning())
                    ->setType('DAY')
                    ->setField('time')
                    ->setExpirationMs($expirationMs)/**10 days*/),
            'time'
        );

        $this->assertNotNull($tableInfo->getMeta());
        $meta = $tableInfo->getMeta()->unpack();
        $this->assertInstanceOf(TableInfo\BigQueryTableMeta::class, $meta);
        $this->assertNotNull($meta->getClustering());
        $this->assertSame(['id'], ProtobufHelper::repeatedStringToArray($meta->getClustering()->getFields()));
        $this->assertNull($meta->getRangePartitioning());
        $this->assertNotNull($meta->getTimePartitioning());
        $this->assertSame('DAY', $meta->getTimePartitioning()->getType());
        $this->assertSame('time', $meta->getTimePartitioning()->getField());
        $this->assertSame($expirationMs, $meta->getTimePartitioning()->getExpirationMs());
    }

    public function testCreateTableFail(): void
    {
        $this->expectException(BadTableDefinitionException::class);
        $this->expectExceptionMessage('Failed to create table');
        $this->createTableForPartitioning(
            (new CreateTableCommand\BigQueryTableMeta())
                // range partitioning must have range defined
                ->setRangePartitioning((new RangePartitioning())
                    ->setField('id')
                    ->setRange((new RangePartitioning\Range()))),
            'range'
        );
    }

    public function testCreateTableFailOnInvalidLength(): void
    {
        $tableName = md5($this->getName());
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setNullable(false)
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = (new TableColumnShared)
            ->setName('time')
            ->setType(Bigquery::TYPE_STRUCT)
            ->setLength('x x x');

        $this->expectException(BadTableDefinitionException::class);
        $this->expectExceptionMessage('Failed to create table');
        $handler(
            $this->projectCredentials,
            (new CreateTableCommand())
                ->setPath($path)
                ->setTableName($tableName)
                ->setColumns($columns),
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );
    }

    private function createTableForPartitioning(
        CreateTableCommand\BigQueryTableMeta $meta,
        string $nameSuffix
    ): TableInfo {
        $tableName = md5($this->getName()) . $nameSuffix;
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, TableColumnShared::class);
        $columns[] = (new TableColumnShared)
            ->setName('id')
            ->setNullable(false)
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = (new TableColumnShared)
            ->setName('time')
            ->setType(Bigquery::TYPE_TIMESTAMP)
            ->setNullable(false);
        $any = new Any();
        $any->pack($meta);
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns)
            ->setMeta($any);
        /** @var ObjectInfoResponse $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            [],
            new RuntimeOptions(['runId' => $this->testRunId]),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());
        $this->assertNotNull($response->getTableInfo());
        return $response->getTableInfo();
    }
}
