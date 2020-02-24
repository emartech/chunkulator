<?php

namespace Emartech\Chunkulator\Notifier;

use Interop\Amqp\AmqpConsumer;
use Interop\Amqp\AmqpMessage;
use Emartech\Chunkulator\Request\ChunkRequestBuilder;
use Emartech\Chunkulator\Request\Request;
use Emartech\Chunkulator\QueueFactory;

class Calculation
{
    private $resultHandler;
    private $calculationRequest;

    /** @var AmqpMessage[] */
    private $messages = [];


    public function __construct(ResultHandler $resultHandler, Request $calculationRequest)
    {
        $this->calculationRequest = $calculationRequest;
        $this->resultHandler = $resultHandler;
    }

    public function addFinishedChunk(int $chunkId, AmqpMessage $message)
    {
        $this->addMessage($chunkId, $message);
    }

    public function finish(AmqpConsumer $consumer, Consumer $calculationContainer)
    {
        if ($this->allChunksDone()) {
            $this->resultHandler->onSuccess($this->calculationRequest->getData());
            $this->ackMessages($consumer);
            $calculationContainer->removeCalculation($this->calculationRequest->getRequestId());
        }
    }

    public function allChunksDone(): bool
    {
        return empty(array_diff($this->allChunkIds(), array_keys($this->messages)));
    }

    private function ackMessages(AmqpConsumer $consumer): void
    {
        foreach ($this->messages as $message) {
            $consumer->acknowledge($message);
        }
    }

    public function requeue(): void
    {
        foreach ($this->messages as $message) {
            $message->publish();
            $message->discard();
        }
    }

    public function discard(AmqpConsumer $consumer): void
    {
        foreach ($this->messages as $message) {
            $consumer->reject($message, false);
        }
    }

    private function allChunkIds()
    {
        return range(0, $this->calculationRequest->getChunkCount() - 1);
    }

    public function retryNotification(QueueFactory $queueFactory, AmqpConsumer $consumer)
    {
        $context = $queueFactory->createContext();
        $notifierQueue = $queueFactory->createNotifierQueue($context);
        $producer = $context->createProducer();

        foreach ($this->messages as $chunkId => $message) {
            $chunk = ChunkRequestBuilder::fromMessage($message);
            if ($chunk->tries > 0) {
                $chunk->tries--;
            }
            $producer->send($notifierQueue, $context->createMessage($chunk->toJson()));
        }
        $this->discard($consumer);

        $context->close();
    }

    private function addMessage(int $chunkId, AmqpMessage $message): void
    {
        $this->messages[$chunkId] = $message;
    }
}
