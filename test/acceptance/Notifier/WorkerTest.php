<?php

namespace Emartech\Chunkulator\Test\Notifier;

use Emartech\Chunkulator\Notifier\Worker;
use Emartech\Chunkulator\Test\Helpers\CalculationRequest;
use Emartech\Chunkulator\Test\IntegrationBaseTestCase;

class WorkerTest extends IntegrationBaseTestCase
{
    /** @var Worker */
    private $notifier;

    protected function setUp(): void
    {
        parent::setUp();
        $this->notifier = new Worker($this->resourceFactory);
    }

    /**
     * @test
     */
    public function run_EmptyQueue_MessagesNotRemoved()
    {
        $this->resultHandler->expects($this->never())->method('onSuccess');
        $this->sendMessage($this->notifierQueue, CalculationRequest::createChunkRequest(2, 1, 0)->toJson());
        $this->notifier->run($this->logger);
        $this->assertCount(1, $this->getMessagesFromQueue($this->notifierQueue));
    }

    /**
     * @test
     */
    public function run_finishedProcessFound_ClientNotifiedAndMessageRemoved()
    {
        $this->resultHandler->expects($this->once())->method('onSuccess');
        $this->sendMessage($this->notifierQueue, CalculationRequest::createChunkRequest(1, 1, 0)->toJson());
        $this->notifier->run($this->logger);
        $this->assertCount(0, $this->getMessagesFromQueue($this->notifierQueue));
    }
}
