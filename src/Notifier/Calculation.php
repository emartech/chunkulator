<?php

namespace Emartech\Chunkulator\Notifier;

use Emartech\AmqpWrapper\Message;
use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\Exception;

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
            try {
                $this->resultHandler->onSuccess($this->calculationRequest->getData());
                $this->ackMessages();
                $calculationContainer->removeCalculation($this->calculationRequest->getRequestId());
            } catch (Exception $ex) {
                $this->discard();
                throw $ex;
            }
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
}
