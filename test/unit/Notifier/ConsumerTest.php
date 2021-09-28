<?php

namespace Emartect\Chunkulator\Test\Unit;

use Emartech\Chunkulator\QueueFactory;
use Emartech\TestHelper\BaseTestCase;
use Emartech\Chunkulator\Exception as ResultHandlerException;
use Emartech\Chunkulator\Notifier\Consumer;
use Emartech\Chunkulator\ResultHandler;
use Emartech\Chunkulator\Request\ChunkRequest;
use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\Test\Helpers\CalculationRequest;
use Exception;
use Interop\Amqp\AmqpConsumer;
use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpMessage;
use Interop\Amqp\AmqpProducer;
use Interop\Amqp\AmqpQueue;
use PHPUnit\Framework\MockObject\Builder\InvocationMocker;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\MockObject\Rule\InvocationOrder;
use Psr\Log\LoggerInterface;

class ConsumerTest extends BaseTestCase
{
    /** @var LoggerInterface|MockObject */
    private $spyLogger;

    /** @var Consumer */
    private $consumer;

    /** @var ResultHandler|MockObject */
    private $resultHandler;

    /** @var AmqpQueue|MockObject */
    private $notificationQueue;

    /** @var AmqpContext|MockObject */
    private $context;

    /** @var AmqpProducer|MockObject */
    private $producer;

    /** @var AmqpConsumer|MockObject */
    private $amqpConsumer;

    protected function setUp(): void
    {
        parent::setUp();
        $this->spyLogger = $this->createMock(LoggerInterface::class);
        $this->resultHandler = $this->createMock(ResultHandler::class);
        $queueFactory = $this->mockQueues();
        $this->consumer = new Consumer(
            $this->resultHandler,
            $this->spyLogger,
            $queueFactory
        );
    }

    /**
     * @test
     */
    public function consume_SingleChunkCalculation_BatchRunResumedAndMessageAcked(): void
    {
        $chunkRequest = CalculationRequest::createChunkRequest(1, 1, 0);

        $mockMessage = $this->createMessage($chunkRequest);
        $this->amqpConsumer->expects($this->once())->method('acknowledge')->with($mockMessage);

        $this->expectSuccessNotificationRequest()->with($chunkRequest->getCalculationRequest()->getData());

        $this->consumer->consume($this->amqpConsumer, $mockMessage);
    }

    /**
     * @test
     */
    public function consume_SuccessNotificationFailsRetryCountNotReached_MessageRequeuedRetryCountDecreased(): void
    {
        $mockMessage = $this->createMessage(CalculationRequest::createChunkRequest(1, 1, 0));
        $this->context
            ->expects($this->once())
            ->method('createMessage')
            ->willReturn($mockMessage);

        $messageWithDecreasedRetries = $this->createMessage(CalculationRequest::createChunkRequest(1, 1, 0, Request::MAX_RETRY_COUNT - 1));

        $ex = new ResultHandlerException();
        $this->expectSuccessNotificationRequest()->willThrowException($ex);

        $this->amqpConsumer->expects($this->never())->method('acknowledge')->with($mockMessage);
        $this->amqpConsumer->expects($this->once())->method('reject')->with($mockMessage, false);

        $this->producer->expects($this->once())->method('send')->with($this->notificationQueue, $messageWithDecreasedRetries);

        $this->consumer->consume($this->amqpConsumer, $mockMessage);
    }

    /**
     * @test
     */
    public function consume_ConsumeFailsWithGenericException_ErrorLoggedExceptionThrown(): void
    {
        $mockMessage = $this->createMessage(CalculationRequest::createChunkRequest(1, 1, 0));

        $ex = new Exception();
        $this->expectSuccessNotificationRequest()->willThrowException($ex);

        $this->amqpConsumer->expects($this->never())->method('acknowledge')->with($mockMessage);
        $this->amqpConsumer->expects($this->never())->method('reject');

        $this->assertExceptionThrown($this->identicalTo($ex), function () use ($mockMessage) {
            $this->consumer->consume($this->amqpConsumer, $mockMessage);
        });
    }

    /**
     * @test
     */
    public function consume_MultipleFinishedCalculations_SuccessNotificationForAllAndMessagesAcked(): void
    {
        $calculationRequest1 = CalculationRequest::createCalculationRequest(2, 1, 'trigger1');
        $calculation1ChunkRequest1 = new ChunkRequest($calculationRequest1, 0, Request::MAX_RETRY_COUNT);
        $calculation1ChunkRequest2 = new ChunkRequest($calculationRequest1, 1, Request::MAX_RETRY_COUNT);
        $calculationRequest2 = CalculationRequest::createCalculationRequest(2, 1, 'trigger2');
        $calculation2ChunkRequest1 = new ChunkRequest($calculationRequest2, 0, Request::MAX_RETRY_COUNT);
        $calculation2ChunkRequest2 = new ChunkRequest($calculationRequest2, 1, Request::MAX_RETRY_COUNT);

        $calculation1chunkMessage1 = $this->createMessage($calculation1ChunkRequest1);
        $calculation1chunkMessage2 = $this->createMessage($calculation1ChunkRequest2);
        $calculation2chunkMessage1 = $this->createMessage($calculation2ChunkRequest1);
        $calculation2chunkMessage2 = $this->createMessage($calculation2ChunkRequest2);

        $this->expectSuccessNotificationRequest($this->at(0))->with($calculationRequest1->getData());
        $this->expectSuccessNotificationRequest($this->at(1))->with($calculationRequest2->getData());

        $this->amqpConsumer->expects($this->exactly(4))->method('acknowledge')->withConsecutive(
            [$calculation1chunkMessage1],
            [$calculation1chunkMessage2],
            [$calculation2chunkMessage1],
            [$calculation2chunkMessage2]
        );

        $this->consumer->consume($this->amqpConsumer, $calculation1chunkMessage1);
        $this->consumer->consume($this->amqpConsumer, $calculation1chunkMessage2);
        $this->consumer->consume($this->amqpConsumer, $calculation2chunkMessage1);
        $this->consumer->consume($this->amqpConsumer, $calculation2chunkMessage2);
    }

    /**
     * @test
     */
    public function finishCalculations_FinishedAndUnfinishedCalculationsExist_BothProcessedCorrectly(): void
    {
        $calculationRequest1 = CalculationRequest::createCalculationRequest(2, 1, 'trigger1');
        $calculation1ChunkRequest1 = new ChunkRequest($calculationRequest1, 0, Request::MAX_RETRY_COUNT);
        $calculation1ChunkRequest2 = new ChunkRequest($calculationRequest1, 1, Request::MAX_RETRY_COUNT);
        $calculationRequest2 = CalculationRequest::createCalculationRequest(2, 1, 'trigger2');
        $calculation2ChunkRequest1 = new ChunkRequest($calculationRequest2, 0, Request::MAX_RETRY_COUNT);

        $calculation1chunkMessage1 = $this->createMessage($calculation1ChunkRequest1);
        $calculation1chunkMessage2 = $this->createMessage($calculation1ChunkRequest2);
        $calculation2chunkMessage1 = $this->createMessage($calculation2ChunkRequest1);

        $this->expectSuccessNotificationRequest()->with($calculationRequest1->getData());

        $this->amqpConsumer->expects($this->never())->method('reject');
        $this->amqpConsumer->expects($this->exactly(2))->method('acknowledge')->withConsecutive(
            [$calculation1chunkMessage1],
            [$calculation1chunkMessage2]
        );

        $this->consumer->consume($this->amqpConsumer, $calculation1chunkMessage1);
        $this->consumer->consume($this->amqpConsumer, $calculation1chunkMessage2);
        $this->consumer->consume($this->amqpConsumer, $calculation2chunkMessage1);
    }

    /**
     * @test
     */
    public function finishCalculations_UnrelatedUnfinishedCalculations_BatchRunsNotResumedMessagesUntouched(): void
    {
        $calculationRequest1 = CalculationRequest::createCalculationRequest(3, 1, 'trigger1');
        $calculation1ChunkRequest1 = new ChunkRequest($calculationRequest1, 0, Request::MAX_RETRY_COUNT);
        $calculation1ChunkRequest2 = new ChunkRequest($calculationRequest1, 1, Request::MAX_RETRY_COUNT);
        $calculationRequest2 = CalculationRequest::createCalculationRequest(2, 1, 'trigger2');
        $calculation2ChunkRequest1 = new ChunkRequest($calculationRequest2, 0, Request::MAX_RETRY_COUNT);

        $calculation1chunkMessage1 = $this->createMessage($calculation1ChunkRequest1);
        $calculation1chunkMessage2 = $this->createMessage($calculation1ChunkRequest2);
        $calculation2chunkMessage1 = $this->createMessage($calculation2ChunkRequest1);

        $this->amqpConsumer->expects($this->never())->method('reject');
        $this->amqpConsumer->expects($this->never())->method('acknowledge');

        $this->expectSuccessNotificationRequest($this->never());

        $this->consumer->consume($this->amqpConsumer, $calculation1chunkMessage1);
        $this->consumer->consume($this->amqpConsumer, $calculation1chunkMessage2);
        $this->consumer->consume($this->amqpConsumer, $calculation2chunkMessage1);
    }

    /**
     * @return AmqpMessage|MockObject
     */
    private function createMessage(ChunkRequest $chunkRequest)
    {
        $message = $this->createMock(AmqpMessage::class);
        $message
            ->expects($this->any())
            ->method('getBody')
            ->willReturn($chunkRequest->toJson());

        return $message;
    }

    private function expectSuccessNotificationRequest(InvocationOrder $when = null): InvocationMocker
    {
        return $this->resultHandler->expects($when ?: $this->once())->method('onAllChunksDone');
    }

    private function mockQueues(): QueueFactory
    {
        $this->notificationQueue = $this->createMock(AmqpQueue::class);
        $this->context = $this->createMock(AmqpContext::class);
        $this->producer = $this->createMock(AmqpProducer::class);
        $this->amqpConsumer = $this->createMock(AmqpConsumer::class);

        $queueFactory = $this->createMock(QueueFactory::class);
        $queueFactory
            ->expects($this->any())
            ->method('createNotifierQueue')
            ->willReturn($this->notificationQueue);

        $queueFactory
            ->expects($this->any())
            ->method('createContext')
            ->willReturn($this->context);

        $this->context
            ->expects($this->any())
            ->method('createProducer')
            ->willReturn($this->producer);

        $this->context
            ->expects($this->any())
            ->method('createConsumer')
            ->willReturn($this->amqpConsumer);

        return $queueFactory;
    }
}
