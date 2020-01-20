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
                $this->calculationRequest->removeFromContainer($calculationContainer);
            } catch (Exception $ex) {
                $this->defer();
                throw $ex;
            }
        }
    }

    public function allChunksDone(): bool
    {
        return empty(array_diff($this->calculationRequest->allChunkIds(), array_keys($this->messages)));
    }

    private function ackMessages(): void
    {
        foreach ($this->messages as $message) {
            $message->ack();
        }
    }

    public function defer(): void
    {
        foreach ($this->messages as $message) {
            $message->publish();
            $message->discard();
        }
    }
}
