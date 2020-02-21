<?php

namespace Emartech\Chunkulator\Test\Calculator;

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

    /** @var RequestStore */
    private $requestStore;


    protected function setUp(): void
    {
        parent::setUp();

        $this->workerConsumer = new SpyConsumer($this);
        $this->notifierConsumer = new SpyConsumer($this);

        $this->requestStore = new RequestStore(
            self::CHUNK_SIZE,
            new ChunkGenerator(),
            $this->queueFactory
        );
    }

    /**
     * @test
     */
    public function createChunks_userListIsEmpty_enqueuesChunkRequestInNotifierQueue()
    {
        $this->requestStore->storeRequest(0, [], self::TRIGGER_ID);

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
        $this->requestStore->storeRequest(2, [], self::TRIGGER_ID);

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
