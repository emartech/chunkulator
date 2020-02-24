<?php

namespace Emartech\Chunkulator\Test\Calculator;

use Emartech\Chunkulator\Initiator\ChunkGenerator;
use Emartech\Chunkulator\Initiator\RequestStore;
use Emartech\Chunkulator\Test\IntegrationBaseTestCase;


class RequestStoreTest extends IntegrationBaseTestCase
{
    const CHUNK_SIZE = 1;
    const TRIGGER_ID = 'trigger_id';

    /** @var RequestStore */
    private $requestStore;


    protected function setUp(): void
    {
        parent::setUp();

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

        $workerMessages = $this->getMessagesFromQueue('worker');
        $notifierMessages = $this->getMessagesFromQueue('notifier');

        $this->assertEmpty($workerMessages);
        $this->assertEquals([
            'requestId' => self::TRIGGER_ID,
            'chunkSize' => self::CHUNK_SIZE,
            'chunkCount' => 1,
            'data' => [],
            'chunkId' => 0,
            'tries' => 3,
        ], json_decode($notifierMessages[0]->getBody(), true));
    }

    /**
     * @test
     */
    public function createChunks_userListShouldBeChunked_enqueuesChunkRequestInWorkerQueue()
    {
        $this->requestStore->storeRequest(2, [], self::TRIGGER_ID);

        $workerMessages = $this->getMessagesFromQueue('worker');
        $notifierMessages = $this->getMessagesFromQueue('notifier');

        $this->assertEquals([
            'requestId' => self::TRIGGER_ID,
            'chunkSize' => self::CHUNK_SIZE,
            'chunkCount' => 2,
            'data' => [],
            'chunkId' => 0,
            'tries' => 3,
        ], json_decode($workerMessages[0]->getBody(), true));

        $this->assertEquals([
            'requestId' => self::TRIGGER_ID,
            'chunkSize' => self::CHUNK_SIZE,
            'chunkCount' => 2,
            'data' => [],
            'chunkId' => 1,
            'tries' => 3,
        ], json_decode($workerMessages[1]->getBody(), true));

        $this->assertEmpty($notifierMessages);
    }
}
