<?php

namespace Emartech\Chunkulator\Calculator;

use Emartech\Chunkulator\Request\ChunkRequestBuilder;
use Emartech\Chunkulator\ResultHandler;
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

    private function calculate(ChunkRequest $request)
    {
        $requestData = $request->getCalculationRequest()->getData();

        $processedContactIds = $this->filter->filterContacts($requestData, $this->getContactsOfChunk($request));
        $this->contactLists->applyContactsToList($requestData, $processedContactIds);

        $this->sendFinishNotification($request);
    }

    public function process(Message $message, Context $context): string
    {
        $request = ChunkRequestBuilder::fromMessage($message);

        try {
            $this->calculate($request);
        } catch (Throwable $t) {
            $this->resultHandler->onChunkError($request->getCalculationRequest()->getData(), $t);

            if ($request->tries > 0) {
                $this->retry($context, $request);
            } else {
                $this->resultHandler->onChunkErrorWithNoTriesLeft($request->getCalculationRequest()->getData(), $t);
                $this->error($context, $message);
            }

            return self::REJECT;
        }

        return self::ACK;
    }

    private function retry(Context $context, ChunkRequest $request)
    {
        $workerQueue = $this->queueFactory->createWorkerQueue($context);
        $request->tries--;
        $context->createProducer()->send($workerQueue, $context->createMessage($request->toJson()));
    }

    private function error(Context $context, Message $message)
    {
        $errorQueue = $this->queueFactory->createErrorQueue($context);
        $context->createProducer()->send($errorQueue, $message);
    }

    public function timeOut()
    {
    }

    private function sendFinishNotification(ChunkRequest $request)
    {
        $context = $this->queueFactory->createContext();
        $queue = $this->queueFactory->createNotifierQueue($context);
        $context->createProducer()->send($queue, $context->createMessage($request->toJson()));
        $context->close();
    }

    private function getContactsOfChunk(ChunkRequest $request): array
    {
        return $this->contactLists->getContactsOfList(
            $request->getCalculationRequest()->getData(),
            $request->getCalculationRequest()->chunkSize,
            $request->getCalculationRequest()->getChunkOffset($request->getChunkId())
        );
    }
}
