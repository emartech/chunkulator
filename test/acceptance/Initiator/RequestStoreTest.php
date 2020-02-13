<?php

namespace Emartech\Chunkulator\Test\Calculator;

use Emartech\Chunkulator\Initiator\ChunkCalculator;
use Emartech\Chunkulator\Initiator\ChunkGenerator;
use Emartech\Chunkulator\Initiator\RequestStore;
use Emartech\Chunkulator\Test\IntegrationBaseTestCase;
use Test\helper\SpyConsumer;


class RequestStoreTest extends IntegrationBaseTestCase
{
    const CHUNK_SIZE = 1;
    const TRIGGER_ID = 'trigger_id';

    /** @var SpyConsumer */
    private $workerConsumer;

    /** @var SpyConsumer */
    private $notifierConsumer;


    protected function setUp(): void
    {
        $this->workerConsumer = new SpyConsumer($this);
        $this->notifierConsumer = new SpyConsumer($this);

        parent::setUp();
    }

    /**
     * @test
     */
    public function createChunks_userListIsEmpty_enqueuesChunkRequestInNotifierQueue()
    {
        $requestStore = new RequestStore(
            new ChunkCalculator(self::CHUNK_SIZE),
            new ChunkGenerator($this->workerQueue, $this->notifierQueue)
        );
        $requestStore->storeRequest(0, [], self::TRIGGER_ID);

        $this->workerQueue->consume($this->workerConsumer);
        $this->notifierQueue->consume($this->notifierConsumer);

        $this->assertEmpty($this->workerConsumer->consumedMessages);
        $this->assertEquals([
            'requestId' => self::TRIGGER_ID,
            'chunkSize' => self::CHUNK_SIZE,
            'chunkCount' => 1,
            'data' => [],
            'chunkId' => 0,
            'tries' => 3,
        ], $this->notifierConsumer->consumedMessages[0]->getContents());
    }

    /**
     * @test
     */
    public function createChunks_userListShouldBeChunked_enqueuesChunkRequestInWorkerQueue()
    {
        $requestStore = new RequestStore(
            new ChunkCalculator(1),
            new ChunkGenerator($this->workerQueue, $this->notifierQueue)
        );
        $requestStore->storeRequest(2, [], self::TRIGGER_ID);

        $this->workerQueue->consume($this->workerConsumer);
        $this->notifierQueue->consume($this->notifierConsumer);

        $this->assertEquals([
            'requestId' => self::TRIGGER_ID,
            'chunkSize' => self::CHUNK_SIZE,
            'chunkCount' => 2,
            'data' => [],
            'chunkId' => 0,
            'tries' => 3,
        ], $this->workerConsumer->consumedMessages[0]->getContents());

        $this->assertEquals([
            'requestId' => self::TRIGGER_ID,
            'chunkSize' => self::CHUNK_SIZE,
            'chunkCount' => 2,
            'data' => [],
            'chunkId' => 1,
            'tries' => 3,
        ], $this->workerConsumer->consumedMessages[1]->getContents());

        $this->assertEmpty($this->notifierConsumer->consumedMessages);
    }
}
