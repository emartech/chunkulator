<?php

namespace Emartech\Chunkulator\Notifier;

use Emartech\AmqpWrapper\Message;
use Emartech\AmqpWrapper\Queue;
use Emartech\Chunkulator\Request\ChunkRequestBuilder;
use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\Exception;
use Throwable;

class Calculation
{
    private $resultHandler;
    private $calculationRequest;

    /** @var Message[] */
    private $messages = [];


    public function __construct(ResultHandler $resultHandler, Request $calculationRequest)
    {
        $this->calculationRequest = $calculationRequest;
        $this->resultHandler = $resultHandler;
    }

    public function addFinishedChunk(int $chunkId, Message $message)
    {
        $this->messages[$chunkId] = $message;
    }

    /**
     * @param Consumer $calculationContainer
     * @throws Exception
     */
    public function finish(Consumer $calculationContainer)
    {
        if ($this->allChunksDone()) {
            $this->resultHandler->onSuccess($this->calculationRequest->getData());
            $this->ackMessages();
            $calculationContainer->removeCalculation($this->calculationRequest->getRequestId());
        }
    }

    public function allChunksDone(): bool
    {
        return empty(array_diff($this->allChunkIds(), array_keys($this->messages)));
    }

    private function ackMessages(): void
    {
        foreach ($this->messages as $message) {
            $message->ack();
        }
    }

    public function requeue(): void
    {
        foreach ($this->messages as $message) {
            $message->publish();
            $message->discard();
        }
    }

    public function discard(): void
    {
        foreach ($this->messages as $message) {
            $message->discard();
        }
    }

    private function allChunkIds()
    {
        return range(0, $this->calculationRequest->getChunkCount() - 1);
    }

    public function retryNotification(Queue $notifierQueue)
    {
        foreach ($this->messages as $message) {
            $this->retryChunk($notifierQueue, $message);
        }
        $this->discard();
    }

    private function retryChunk(Queue $notifierQueue, Message $message): void
    {
        $request = ChunkRequestBuilder::fromMessage($message);
        if ($request->tries > 0) {
            $request->tries--;
            $request->enqueueIn($notifierQueue);
        }
    }
}
