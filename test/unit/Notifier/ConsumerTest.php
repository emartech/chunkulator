<?php

namespace Emartect\Chunkulator\Test\Unit;

use Emartech\AmqpWrapper\Message;
use Emartech\TestHelper\BaseTestCase;
use Emartech\Chunkulator\Exception;
use Emartech\Chunkulator\Notifier\Consumer;
use Emartech\Chunkulator\Notifier\ResultHandler;
use Emartech\Chunkulator\Request\ChunkRequest;
use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\Test\Helpers\CalculationRequest;
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

    protected function setUp(): void
    {
        parent::setUp();
        $this->spyLogger = $this->createMock(LoggerInterface::class);
        $this->resultHandler = $this->createMock(ResultHandler::class);
        $this->consumer = new Consumer(
            $this->resultHandler,
            $this->spyLogger
        );
    }

    /**
     * @test
     */
    public function finishCalculations_SingleChunkCalculation_BatchRunResumedAndMessageAcked(): void
    {
        $chunkRequest = CalculationRequest::createChunkRequest(1, 1, 0);

        $mockMessage = $this->createMessage($chunkRequest);
        $mockMessage->expects($this->once())->method('ack');

        $this->expectSuccessNotificationRequest()->with($chunkRequest->getCalculationRequest()->getData());

        $this->consumer->addMessage($mockMessage);
        $this->consumer->finishCalculations();
    }

    /**
     * @test
     */
    public function finishCalculations_NoChunks_NothingIsNotified(): void
    {
        $this->expectSuccessNotificationRequest($this->never());
        $this->consumer->finishCalculations();
    }

    /**
     * @test
     */
    public function finishCalculations_SuccessNotificationFails_ErrorLoggedMessageRepublishedExceptionThrown(): void
    {
        $mockMessage = $this->createMessage(CalculationRequest::createChunkRequest(1, 1, 0));
        $this->consumer->addMessage($mockMessage);

        $ex = new Exception();
        $this->expectSuccessNotificationRequest()->willThrowException($ex);

        $mockMessage->expects($this->never())->method('ack');
        $mockMessage->expects($this->at(0))->method('publish');
        $mockMessage->expects($this->at(1))->method('discard');

        $this->assertExceptionThrown($this->identicalTo($ex), function () {
            $this->consumer->finishCalculations();
        });
    }

    /**
     * @test
     */
    public function finishCalculations_MultipleFinishedCalculations_SuccessNotificationForAllAndMessagesAcked(): void
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

        $this->consumer->addMessage($calculation1chunkMessage1);
        $this->consumer->addMessage($calculation1chunkMessage2);
        $this->consumer->addMessage($calculation2chunkMessage1);
        $this->consumer->addMessage($calculation2chunkMessage2);

        $this->expectSuccessNotificationRequest($this->at(0))->with($calculationRequest1->getData());
        $this->expectSuccessNotificationRequest($this->at(1))->with($calculationRequest2->getData());

        $calculation1chunkMessage1->expects($this->once())->method('ack');
        $calculation1chunkMessage2->expects($this->once())->method('ack');
        $calculation2chunkMessage1->expects($this->once())->method('ack');
        $calculation2chunkMessage2->expects($this->once())->method('ack');

        $this->consumer->finishCalculations();
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

        $this->consumer->addMessage($calculation1chunkMessage1);
        $this->consumer->addMessage($calculation1chunkMessage2);
        $this->consumer->addMessage($calculation2chunkMessage1);

        $this->expectSuccessNotificationRequest()->with($calculationRequest1->getData());

        $calculation1chunkMessage1->expects($this->once())->method('ack');
        $calculation1chunkMessage2->expects($this->once())->method('ack');
        $calculation1chunkMessage1->expects($this->never())->method('reject');
        $calculation1chunkMessage2->expects($this->never())->method('reject');
        $calculation2chunkMessage1->expects($this->never())->method('ack');
        $calculation2chunkMessage1->expects($this->never())->method('reject');

        $this->consumer->finishCalculations();
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

        $this->consumer->addMessage($calculation1chunkMessage1);
        $this->consumer->addMessage($calculation1chunkMessage2);
        $this->consumer->addMessage($calculation2chunkMessage1);

        $calculation1chunkMessage1->expects($this->never())->method('ack');
        $calculation1chunkMessage2->expects($this->never())->method('ack');
        $calculation1chunkMessage1->expects($this->never())->method('reject');
        $calculation1chunkMessage2->expects($this->never())->method('reject');
        $calculation2chunkMessage1->expects($this->never())->method('ack');
        $calculation2chunkMessage1->expects($this->never())->method('reject');

        $this->expectSuccessNotificationRequest($this->never());

        $this->consumer->finishCalculations();
    }

    /**
     * @return Message|MockObject
     */
    private function createMessage(ChunkRequest $chunkRequest)
    {
        $message = $this->createMock(Message::class);
        $message
            ->expects($this->any())
            ->method('getContents')
            ->willReturn(
                $chunkRequest->getMessageData()
            );

        return $message;
    }

    private function expectSuccessNotificationRequest(InvocationOrder $when = null): InvocationMocker
    {
        return $this->resultHandler->expects($when ?: $this->once())->method('onSuccess');
    }
}
