<?php

namespace Emartech\Chunkulator\Test\Acceptance;

use Emartech\Chunkulator\Exception as ResultHandlerException;
use Emartech\Chunkulator\Notifier\Calculation;
use Emartech\Chunkulator\Notifier\Consumer;
use Emartech\Chunkulator\Notifier\ResultHandler;
use Emartech\Chunkulator\Test\Helpers\CalculationRequest;
use Emartech\Chunkulator\Test\IntegrationBaseTestCase;
use PHPUnit\Framework\MockObject\MockObject;
use Test\helper\SpyConsumer;

class CalculationTest extends IntegrationBaseTestCase
{
    /**
     * @var SpyConsumer
     */
    private $spyConsumer;

    /**
     * @var ResultHandler|MockObject
     */
    private $resultHandler;

    protected function setUp(): void
    {
        parent::setUp();
        $this->spyConsumer = new SpyConsumer($this);
        $this->resultHandler = $this->createMock(ResultHandler::class);

    }

    /**
     * @test
     */
    public function calculation_SuccessHandlerFails_MessageDiscarded()
    {
        $ex = new ResultHandlerException();
        $this->resultHandler->expects($this->once())->method('onSuccess')->willThrowException($ex);

        $this->notifierQueue->send(['test message']);
        $this->notifierQueue->consume($this->spyConsumer);
        $message = $this->spyConsumer->consumedMessages[0];

        $this->notifierQueue->send(['other message']);

        $calculation = new Calculation($this->resultHandler, CalculationRequest::createCalculationRequest(1, 1));
        $calculation->addFinishedChunk(0, $message);
        $this->assertExceptionThrown($this->identicalTo($ex), function () use ($calculation) {
            $calculation->finish($this->createMock(Consumer::class));
        });

        $this->notifierQueue->consume($this->spyConsumer);
        $this->assertCount(2, $this->spyConsumer->consumedMessages);
        $message2 = $this->spyConsumer->consumedMessages[1];

        $this->assertEquals(['other message'], $message2->getContents());
    }
}
