<?php

namespace Emartech\Chunkulator\Test\Calculator;

use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\Test\Helpers\ResourceFactory;
use Emartech\TestHelper\BaseTestCase;
use Emartech\Chunkulator\Calculator\Consumer;
use Emartech\Chunkulator\Calculator\ContactListHandler;
use Emartech\Chunkulator\Calculator\Filter;
use Emartech\Chunkulator\ResultHandler;
use Emartech\Chunkulator\Request\ChunkRequest;
use Emartech\Chunkulator\Test\Helpers\CalculationRequest;
use Emartech\Chunkulator\Test\Helpers\Constants;
use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpProducer;
use Interop\Amqp\AmqpQueue;
use Interop\Queue\Message;
use Interop\Queue\Processor;
use Interop\Queue\Queue;
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
     * @var Queue|MockObject
     */
    private $errorQueue;

    /**
     * @var Filter|MockObject
     */
    private $filter;

    /**
     * @var ContactListHandler|MockObject
     */
    private $contactListHandler;

    /**
     * @var AmqpContext|MockObject
     */
    private $context;

    /**
     * @var AmqpProducer|MockObject
     */
    private $producer;

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

        $chunkRequest = CalculationRequest::createChunkRequest($chunkCount, 3, 2);
        $message = $this->createMessage($chunkRequest);

        $this->expectSourceContactsToBe($contactIds, Constants::REQUEST_DATA, 3, 6);

        $this->expectFiltering()
            ->with(Constants::REQUEST_DATA, $contactIds)
            ->willReturn($filteredContactIds);

        $this->expectTargetContactToBe(Constants::REQUEST_DATA, $filteredContactIds);

        $this->expectEnqueueToNotifierQueue($chunkRequest);

        $this->assertEquals(Processor::ACK, $this->consumer->process($message, $this->context));
    }

    /**
     * @test
     */
    public function consume_InvalidMessage_ReturnsWithReject()
    {
        $message = $this->createMock(Message::class);
        $message
            ->expects($this->any())
            ->method('getBody')
            ->willReturn('{"invalid":"message"}');

        $this->assertEquals(Processor::REJECT, $this->consumer->process($message, $this->context));
    }

    /**
     * @test
     */
    public function consume_InvalidMessage_PutToErrorQueue()
    {
        $message = $this->createMock(Message::class);
        $message
            ->expects($this->any())
            ->method('getBody')
            ->willReturn('{"invalid":"message"}');

        $this->expectEnqueueToErrorQueue($message);

        $this->consumer->process($message, $this->context);
    }

    /**
     * @test
     */
    public function consume_InvalidMessage_CallsErrorResultHandler()
    {
        $message = $this->createMock(Message::class);
        $message
            ->expects($this->any())
            ->method('getBody')
            ->willReturn('{"invalid":"message"}');

        $this->resultHandler
            ->expects($this->once())
            ->method('onChunkErrorWithNoTriesLeft')
            ->with($message->getBody());

        $this->consumer->process($message, $this->context);
    }

    /**
     * @test
     */
    public function consume_ConsumeError_ReturnsWithReject()
    {
        $chunkRequest = CalculationRequest::createChunkRequest(1, 1, 0, 1);
        $message = $this->createMessage($chunkRequest);

        $this->expectFilteringException();

        $this->assertEquals(Processor::REJECT, $this->consumer->process($message, $this->context));
    }

    /**
     * @test
     */
    public function consume_ConsumeError_MoreTriesLeft_CallsErrorResultHandler()
    {
        $chunkRequest = CalculationRequest::createChunkRequest(1, 1, 0, 1);
        $message = $this->createMessage($chunkRequest);

        $this->expectFilteringException();
        $this->resultHandler
            ->expects($this->once())
            ->method('onChunkError')
            ->with($message->getBody());
        $this->resultHandler->expects($this->never())->method('onChunkErrorWithNoTriesLeft');

        $this->consumer->process($message, $this->context);
    }

    /**
     * @test
     */
    public function consume_ConsumeError_MoreTriesLeft_DecreaseTriesAndRequeue()
    {
        $chunkRequest = CalculationRequest::createChunkRequest(1, 1, 0, 1);
        $message = $this->createMessage($chunkRequest);

        $this->expectFilteringException();
        $this->expectEnqueueToWorkerQueue(
            CalculationRequest::createChunkRequest(1, 1, 0, 0)
        );

        $this->consumer->process($message, $this->context);
    }

    /**
     * @test
     */
    public function consume_ConsumeError_NoMoreTriesLeft_DiscardsMessage()
    {
        $chunkRequest = CalculationRequest::createChunkRequest(2, 1, 0, 0);
        $message = $this->createMessage($chunkRequest);

        $this->expectFilteringException();

        $this->assertEquals(Processor::REJECT, $this->consumer->process($message, $this->context));
    }

    /**
     * @test
     */
    public function consume_ConsumeError_NoMoreTriesLeft_callsBothErrorResultHandler()
    {
        $chunkRequest = CalculationRequest::createChunkRequest(2, 1, 0, 0);
        $message = $this->createMessage($chunkRequest);
        $exception = $this->createMock(Throwable::class);

        $this->expectFilteringException($exception);
        $this->resultHandler
            ->expects($this->once())
            ->method('onChunkError')
            ->with($message->getBody(), $exception);
        $this->resultHandler
            ->expects($this->once())
            ->method('onChunkErrorWithNoTriesLeft')
            ->with($message->getBody(), $exception);

        $this->consumer->process($message, $this->context);
    }

    /**
     * @test
     */
    public function consume_ConsumeError_NoMoreTriesLeft_PutsToErrorQueue()
    {
        $chunkRequest = CalculationRequest::createChunkRequest(2, 1, 0, 0);
        $message = $this->createMessage($chunkRequest);

        $this->expectFilteringException();
        $this->expectEnqueueToErrorQueue($message);

        $this->consumer->process($message, $this->context);
    }

    /**
     * @return MockObject|Message
     */
    private function createMessage(ChunkRequest $chunkRequest)
    {
        $message = $this->createMock(Message::class);
        $message
            ->expects($this->any())
            ->method('getBody')
            ->willReturn($chunkRequest->toJson());

        return $message;
    }

    private function expectEnqueueToNotifierQueue(ChunkRequest $chunkRequest)
    {
        $message = $this->createMessage($chunkRequest);
        $this->context
            ->expects($this->once())
            ->method('createMessage')
            ->with($chunkRequest->toJson())
            ->willReturn($message);

        $this->expectEnqueueToQueue($this->notificationQueue->getQueueName(), $message);
    }

    private function expectEnqueueToWorkerQueue(ChunkRequest $chunkRequest)
    {
        $message = $this->createMessage($chunkRequest);
        $this->context
            ->expects($this->any())
            ->method('createMessage')
            ->with($chunkRequest->toJson())
            ->willReturn($message);

        $this->expectEnqueueToQueue($this->workerQueue->getQueueName(), $message);
    }

    private function expectEnqueueToErrorQueue(Message $message)
    {
        $this->expectEnqueueToQueue($this->errorQueue->getQueueName(), $message);
    }

    private function expectEnqueueToQueue(string $queueName, Message $message)
    {
        $this->producer
            ->expects($this->once())
            ->method('send')
            ->with(
                $this->callback(function (AmqpQueue $queue) use ($queueName) {
                    $this->assertEquals($queueName, $queue->getQueueName());
                    return true;
                }),
                $message
            );
    }

    private function expectSourceContactsToBe(array $contactIds, array $requestData, int $limit, int $offset): void
    {
        $this->contactListHandler
            ->expects($this->once())
            ->method('getContactsOfList')
            ->with($requestData, $limit, $offset)
            ->willReturn($contactIds);
    }

    private function expectTargetContactToBe(array $requestData, array $contactIds): void
    {
        $this->contactListHandler
            ->expects($this->once())
            ->method('applyContactsToList')
            ->with($requestData, $contactIds);
    }

    private function expectFilteringException(Throwable $t = null)
    {
        $this->expectFiltering()->willThrowException($t ?? $this->createMock(Throwable::class));
    }

    private function expectFiltering(): InvocationMocker
    {
        return $this->filter->expects($this->once())->method('filterContacts');
    }

    private function mockQueues(): QueueFactory
    {
        $this->notificationQueue = $this->createMock(AmqpQueue::class);
        $this->workerQueue = $this->createMock(AmqpQueue::class);
        $this->workerQueue->expects($this->any())->method('getQueueName')->willReturn(ResourceFactory::QUEUE_NAME_WORKER);
        $this->errorQueue = $this->createMock(AmqpQueue::class);
        $this->errorQueue->expects($this->any())->method('getQueueName')->willReturn(ResourceFactory::QUEUE_NAME_WORKER);
        $this->context = $this->createMock(AmqpContext::class);
        $this->producer = $this->createMock(AmqpProducer::class);

        $queueFactory = $this->createMock(QueueFactory::class);
        $queueFactory
            ->expects($this->any())
            ->method('createNotifierQueue')
            ->willReturn($this->notificationQueue);

        $queueFactory
            ->expects($this->any())
            ->method('createWorkerQueue')
            ->willReturn($this->workerQueue);

        $queueFactory
            ->expects($this->any())
            ->method('createErrorQueue')
            ->willReturn($this->errorQueue);

        $queueFactory
            ->expects($this->any())
            ->method('createContext')
            ->willReturn($this->context);

        $this->context
            ->expects($this->any())
            ->method('createProducer')
            ->willReturn($this->producer);

        return $queueFactory;
    }
}
