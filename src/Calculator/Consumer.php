<?php

namespace Emartech\Chunkulator\Calculator;

use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\QueueConsumer;
use Emartech\Chunkulator\Request\ChunkRequestBuilder;
use Emartech\Chunkulator\Notifier\ResultHandler;
use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\Request\ChunkRequest;
use Throwable;

class Consumer implements QueueConsumer
{
    private $contactLists;
    private $resultHandler;
    private $queueFactory;
    private $filter;

    public function __construct(
        ContactListHandler $contactLists,
        Filter $filter,
        ResultHandler $resultHandler,
        QueueFactory $queueFactory
    )
    {
        $this->contactLists = $contactLists;
        $this->resultHandler = $resultHandler;
        $this->queueFactory = $queueFactory;
        $this->filter = $filter;
    }

    private function calculate(ChunkRequest $request): void
    {
        $processedContactIds = $this->filter->filterContacts(
            $request->getCalculationRequest()->getData(),
            $this->contactLists->getContactsOfList($request)
        );

        $this->contactLists->applyContactsToList($request, $processedContactIds);
        $this->sendFinishNotification($request);
    }

    public function getPrefetchCount(): int
    {
        return 1;
    }

    public function consume(Message $message): void
    {
        $request = ChunkRequestBuilder::fromMessage($message);

        try {
            $this->calculate($request);
            $message->ack();
        } catch (Throwable $t) {
            $this->resultHandler->onError($request->getCalculationRequest()->getData(), $t);
            $this->retry($message, $request);
        }
    }

    private function retry(Message $message, ChunkRequest $request): void
    {
        $workerQueue = $this->queueFactory->createWorkerQueue();
        if ($request->tries > 0) {
            $request->tries--;
            $request->enqueueIn($workerQueue);
            $message->discard();
        } else {
            try {
                $this->resultHandler->onFailure($request->getCalculationRequest()->getData());
                $message->discard();
            } catch (Throwable $t) {
                $request->enqueueIn($workerQueue);
                $message->discard();
                throw $t;
            }
        }
    }

    public function timeOut(): void
    {
    }

    private function sendFinishNotification(ChunkRequest $request): void
    {
        $request->enqueueIn($this->queueFactory->createNotifierQueue());
        $this->queueFactory->closeNotifierQueue();
    }
}
