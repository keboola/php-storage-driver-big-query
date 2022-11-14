<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\FunctionalTests\UseCase\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\StorageDriver\BigQuery\Handler\Table\Create\CreateTableHandler;
use Keboola\StorageDriver\BigQuery\Handler\Table\Drop\DropTableHandler;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Table\CreateTableCommand;
use Keboola\StorageDriver\Command\Table\DropTableCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\FunctionalTests\BaseCase;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

class CreateDropTableTest extends BaseCase
{
    protected CreateBucketResponse $bucketResponse;

    private GenericBackendCredentials $projectCredentials;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanTestProject();

        [$projectCredentials, $projectResponse] = $this->createTestProject();
        $this->projectCredentials = $projectCredentials;

        $this->bucketResponse = $this->createTestBucket($projectCredentials);
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->cleanTestProject();
    }

    public function testCreateTable(): void
    {
        $tableName = md5($this->getName()) . '_Test_table';
        $bucketDatasetName = $this->bucketResponse->getCreateBucketObjectName();

        // CREATE TABLE
        $handler = new CreateTableHandler($this->clientManager);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $bucketDatasetName;
        $columns = new RepeatedField(GPBType::MESSAGE, CreateTableCommand\TableColumn::class);
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('id')
            ->setType(Bigquery::TYPE_INT64);
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('name')
            ->setType(Bigquery::TYPE_STRING)
            ->setLength('50')
            ->setNullable(true)
            ->setDefault("'Some Default'");
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('large')
            ->setType(Bigquery::TYPE_BIGNUMERIC)
            ->setLength('76,38')
            ->setDefault('185.554');
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('ordes')
            ->setType(Bigquery::TYPE_ARRAY)
            ->setLength('STRUCT<x ARRAY<STRUCT<xz ARRAY<INT64>>>>');
        $columns[] = (new CreateTableCommand\TableColumn())
            ->setName('organization')
            ->setType(Bigquery::TYPE_STRUCT)
            ->setLength('x ARRAY<INT64>');
        $command = (new CreateTableCommand())
            ->setPath($path)
            ->setTableName($tableName)
            ->setColumns($columns);
        /** @var TableInfo $response */
        $response = $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $columns = $response->getColumns();
        $this->assertCount(3, $columns);

        // check column ID
        /** @var TableInfo\TableColumn $column */
        $column = $columns->offsetGet(0);
        $this->assertSame('id', $column->getName());
        $this->assertSame(Bigquery::TYPE_INT64, $column->getType());
        $this->assertFalse($column->getNullable());
        $this->assertSame('', $column->getDefault());

        // check column NAME
        /** @var TableInfo\TableColumn $column */
        $column = $columns->offsetGet(1);
        $this->assertSame('name', $column->getName());
        $this->assertSame(Bigquery::TYPE_STRING, $column->getType());
        $this->assertSame('50', $column->getLength());
        $this->assertTrue($column->getNullable());
        $this->assertSame("'Some Default'", $column->getDefault());

        // check column LARGE
        /** @var TableInfo\TableColumn $column */
        $column = $columns->offsetGet(2);
        $this->assertSame('large', $column->getName());
        $this->assertSame(Bigquery::TYPE_BIGNUMERIC, $column->getType());
        $this->assertSame('76,38', $column->getLength());
        $this->assertFalse($column->getNullable());
        $this->assertSame('185.554', $column->getDefault());

        // check column array
        $column = $columns[3];
        $this->assertSame('ordes', $column->getColumnName());
        $columnDef = $column->getColumnDefinition();
        $this->assertSame(Bigquery::TYPE_ARRAY, $columnDef->getType());
        $this->assertSame('STRUCT<x ARRAY<STRUCT<xz ARRAY<INT64>>>>', $columnDef->getLength());
        $this->assertFalse($columnDef->isNullable());

        // check column array
        $column = $columns[4];
        $this->assertSame('organization', $column->getColumnName());
        $columnDef = $column->getColumnDefinition();
        $this->assertSame(Bigquery::TYPE_STRUCT, $columnDef->getType());
        $this->assertSame('x ARRAY<INT64>', $columnDef->getLength());
        $this->assertTrue($columnDef->isNullable());

        // DROP TABLE
        $handler = new DropTableHandler($this->clientManager);
        $command = (new DropTableCommand())
            ->setPath($path)
            ->setTableName($tableName);

        $handler(
            $this->projectCredentials,
            $command,
            []
        );

        $bqClient = $this->clientManager->getBigQueryClient($this->projectCredentials);

        $bucket = $bqClient->dataset($bucketDatasetName);
        $this->assertTrue($bucket->exists());

        $table = $bucket->table($tableName);
        $this->assertFalse($table->exists());
    }
}
