<?php

namespace Emartech\Chunkulator\Test\Acceptance;

use Emartech\Chunkulator\Exception as ResultHandlerException;
use Emartech\Chunkulator\Notifier\Calculation;
use Emartech\Chunkulator\Notifier\Consumer;
use Emartech\Chunkulator\Notifier\ResultHandler;
use Emartech\Chunkulator\Test\Helpers\CalculationRequest;
use Emartech\Chunkulator\Test\IntegrationBaseTestCase;
use Interop\Amqp\AmqpConsumer;
use PHPUnit\Framework\MockObject\MockObject;

class CalculationTest extends IntegrationBaseTestCase
{
    /**
     * @var ResultHandler|MockObject
     */
    private $resultHandler;

    protected function setUp(): void
    {
        parent::setUp();
        $this->resultHandler = $this->createMock(ResultHandler::class);

    }

    /**
     * @test
     */
    public function calculation_SuccessHandlerFails_MessageDiscarded()
    {
        $ex = new ResultHandlerException();
        $this->resultHandler->expects($this->once())->method('onSuccess')->willThrowException($ex);

        $amqpProducer = $this->context->createProducer();

        $amqpProducer->send($this->notifierQueue, $this->context->createMessage(json_encode(['test message'])));
        $notifierMessages = $this->getMessagesFromQueue('notifier');
        $message = $notifierMessages[0];

        $amqpProducer->send($this->notifierQueue, $this->context->createMessage(json_encode(['other message'])));

        $calculation = new Calculation($this->resultHandler, CalculationRequest::createCalculationRequest(1, 1));
        $calculation->addFinishedChunk(0, $message);
        $this->assertExceptionThrown($this->identicalTo($ex), function () use ($calculation) {
            $calculation->finish($this->createMock(AmqpConsumer::class), $this->createMock(Consumer::class));
        });

        $notifierMessages = $this->getMessagesFromQueue('notifier');
        $this->assertCount(2, $notifierMessages);
        $message2 = $notifierMessages[1];

        $this->assertEquals(['other message'], json_decode($message2->getBody(), true));
    }
}
