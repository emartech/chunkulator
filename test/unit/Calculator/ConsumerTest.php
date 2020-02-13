<?php

namespace Emartech\Chunkulator\Test\Calculator;

use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\Queue;
use Emartech\TestHelper\BaseTestCase;
use Emartech\Chunkulator\Calculator\Consumer;
use Emartech\Chunkulator\Calculator\ContactListHandler;
use Emartech\Chunkulator\Calculator\Filter;
use Emartech\Chunkulator\Notifier\ResultHandler;
use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\Request\ChunkRequest;
use Emartech\Chunkulator\Test\Helpers\CalculationRequest;
use Emartech\Chunkulator\Test\Helpers\Constants;
use PHPUnit\Framework\MockObject\Builder\InvocationMocker;
use PHPUnit\Framework\MockObject\MockObject;
use Throwable;

class ConsumerTest extends BaseTestCase
{
    /**
     * @var Consumer
     */
    private $consumer;

    /**
     * @var ResultHandler|MockObject
     */
    private $resultHandler;

    /**
     * @var Queue|MockObject
     */
    private $workerQueue;

    /**
     * @var Queue|MockObject
     */
    private $notificationQueue;

    /**
     * @var Filter|MockObject
     */
    private $filter;

    /**
     * @var ContactListHandler|MockObject
     */
    private $contactListHandler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->contactListHandler = $this->createMock(ContactListHandler::class);
        $this->filter = $this->createMock(Filter::class);
        $this->resultHandler = $this->createMock(ResultHandler::class);

        $this->consumer = new Consumer(
            $this->contactListHandler,
            $this->filter,
            $this->resultHandler,
            $this->mockQueues()
        );
    }

    /**
     * @test
     */
    public function consume_userListIdGivenThirdChunk_getUserListChunkFromSuiteAndEnqueueToNotifier()
    {
        $contactIds = [1, 2, 3];
        $filteredContactIds = [2];
        $chunkCount = 5;

        $request = CalculationRequest::createChunkRequest($chunkCount, 3, 2);
        $message = $this->createMessage($request);

        $this->expectSourceContactsToBe($contactIds, $request);

        $this->expectFiltering()
            ->with($request->getCalculationRequest()->getData(), $contactIds)
            ->willReturn($filteredContactIds);

        $this->expectTargetContactToBe($request, $filteredContactIds);

        $this->expectEnqueueToNotifierQueue($this->structure([
            'requestId' => Constants::TRIGGER_ID,
            'chunkCount' => $this->identicalTo($chunkCount),
            'chunkId' => $this->identicalTo(2),
        ]));

        $message->expects($this->once())->method('ack');

        $this->consumer->consume($message);
    }

    /**
     * @test
     */
    public function consume_MoreTriesLeft_DecreaseTriesAndRequeue()
    {
        $request = CalculationRequest::createChunkRequest(1, 1, 0, 1);
        $message = $this->createMessage($request);

        $this->expectFiltering()->willThrowException($this->createMock(Throwable::class));
        $this->expectEnqueueToWorkerQueue()->with($this->structure(['tries' => 0]));

        $message->expects($this->once())->method('discard');

        $this->resultHandler->expects($this->never())->method('onFailure');

        $this->consumer->consume($message);
    }

    /**
     * @test
     */
    public function consume_NoMoreTriesLeft_DiscardsMessage()
    {
        $request = CalculationRequest::createChunkRequest(2, 1, 0, 0);
        $message = $this->createMessage($request);

        $this->expectFiltering()->willThrowException($this->createMock(Throwable::class));
        $this->expectFailureHandlerCall();

        $message->expects($this->once())->method('discard');

        $this->consumer->consume($message);
    }

    /**
     * @test
     */
    public function consume_NoMoreTriesLeftCallbackFails_Requeue()
    {
        $request = CalculationRequest::createChunkRequest(2, 1, 0, 0);
        $message = $this->createMessage($request);

        $this->expectFiltering()->willThrowException($this->createMock(Throwable::class));
        $this->expectFailureHandlerCall()->willThrowException($this->createMock(Throwable::class));

        $this->expectEnqueueToWorkerQueue()
            ->with($this->structure([
                'tries' => 0
            ]));

        $message->expects($this->once())->method('discard');

        try {
            $this->consumer->consume($message);
        } catch (Throwable $t) {
            return;
        }

        $this->fail();
    }

    /**
     * @return MockObject|Message
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

    private function expectEnqueueToNotifierQueue($expectedQueueItem)
    {
        $this->notificationQueue
            ->expects($this->once())
            ->method('send')
            ->with($expectedQueueItem);
    }

    private function expectSourceContactsToBe($contactIds, ChunkRequest $request): void
    {
        $this->contactListHandler
            ->expects($this->once())
            ->method('getContactsOfList')
            ->with($request)
            ->willReturn($contactIds);
    }

    private function expectTargetContactToBe(ChunkRequest $request, $customerIds): void
    {
        $this->contactListHandler
            ->expects($this->once())
            ->method('applyContactsToList')
            ->with($request, $customerIds);
    }

    private function expectFailureHandlerCall(): InvocationMocker
    {
        return $this->resultHandler->expects($this->once())->method('onFailure');
    }

    private function expectFiltering(): InvocationMocker
    {
        return $this->filter->expects($this->once())->method('filterContacts');
    }

    private function expectEnqueueToWorkerQueue(): InvocationMocker
    {
        return $this->workerQueue->expects($this->once())->method('send');
    }

    private function mockQueues()
    {
        $this->notificationQueue = $this->createMock(Queue::class);
        $this->workerQueue = $this->createMock(Queue::class);

        $queueFactory = $this->createMock(QueueFactory::class);
        $queueFactory
            ->expects($this->any())
            ->method('createNotifierQueue')
            ->willReturn($this->notificationQueue)
        ;

        $queueFactory
            ->expects($this->any())
            ->method('createWorkerQueue')
            ->willReturn($this->workerQueue)
        ;
        return $queueFactory;
    }
}
