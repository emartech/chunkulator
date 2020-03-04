<?php

namespace Emartech\Chunkulator\Calculator;

use Emartech\Chunkulator\Request\ChunkRequestBuilder;
use Emartech\Chunkulator\Notifier\ResultHandler;
use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\Request\ChunkRequest;
use Interop\Queue\Context;
use Interop\Queue\Message;
use Interop\Queue\Processor;
use Throwable;

class Consumer implements Processor
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
        $requestData = $request->getCalculationRequest()->getData();

        $processedContactIds = $this->filter->filterContacts($requestData, $this->getContactsOfChunk($request));
        $this->contactLists->applyContactsToList($requestData, $processedContactIds);

        $this->sendFinishNotification($request);
    }

    public function getPrefetchCount(): int
    {
        return 1;
    }

    public function process(Message $message, Context $context)
    {
        $request = ChunkRequestBuilder::fromMessage($message);

        try {
            $this->calculate($request);
        } catch (Throwable $t) {
            $this->resultHandler->onError($request->getCalculationRequest()->getData(), $t);
            $this->retry($context, $request);
            return self::REJECT;
        }

        return self::ACK;
    }

    private function retry(Context $context, ChunkRequest $request)
    {
        $workerQueue = $this->queueFactory->createWorkerQueue($context);
        if ($request->tries > 0) {
            $request->tries--;
            $context->createProducer()->send($workerQueue, $context->createMessage($request->toJson()));
        } else {
            try {
                $this->resultHandler->onFailure($request->getCalculationRequest()->getData());
            } catch (Throwable $t) {
                $context->createProducer()->send($workerQueue, $context->createMessage($request->toJson()));
                throw $t;
            }
        }
    }

    public function timeOut(): void
    {
    }

    private function sendFinishNotification(ChunkRequest $request): void
    {
        $context = $this->queueFactory->createContext();
        $queue = $this->queueFactory->createNotifierQueue($context);
        $context->createProducer()->send($queue, $context->createMessage($request->toJson()));
        $context->close();
    }

    private function getContactsOfChunk(ChunkRequest $request): array
    {
        return $this->contactLists->getContactsOfList(
            $request->getCalculationRequest()
                ->getData(), $request->getCalculationRequest()->chunkSize, $request->getCalculationRequest()
            ->getChunkOffset($request->getChunkId())
        );
    }
}
