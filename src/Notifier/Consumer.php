<?php

namespace Emartech\Chunkulator\Notifier;

use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\Request\ChunkRequest;
use Emartech\Chunkulator\Request\ChunkRequestBuilder;
use Exception;
use Interop\Amqp\AmqpConsumer;
use Interop\Amqp\AmqpMessage as AmqpMessageInterface;
use Emartech\Chunkulator\Exception as ResultHandlerException;
use Psr\Log\LoggerInterface;


class Consumer
{
    private $resultHandler;
    private $logger;
    private $queueFactory;

    /** @var Calculation[] */
    private $calculations = [];

    public function __construct(ResultHandler $resultHandler, LoggerInterface $logger, QueueFactory $queueFactory)
    {
        $this->resultHandler = $resultHandler;
        $this->logger = $logger;
        $this->queueFactory = $queueFactory;
    }

    public function getPrefetchCount(): ?int
    {
        return null;
    }

    public function consume(AmqpConsumer $consumer, AmqpMessageInterface $message): void
    {
        $chunkRequest = ChunkRequestBuilder::fromMessage($message);
        $calculation = $this->getCalculation($chunkRequest);
        $calculation->addFinishedChunk($chunkRequest->getChunkId(), $message);
        try {
            $calculation->finish($consumer, $this);
        } catch (ResultHandlerException $ex) {
            $this->logger->error('Finishing calculation failed', ['exception' => $ex]);
            $calculation->retryNotification($this->queueFactory, $consumer);
        } catch (Exception $ex) {
            $this->logger->error('Finishing calculation failed', ['exception' => $ex]);
            throw $ex;
        }
    }

    public function timeOut(): void
    {
        foreach ($this->calculations as $requestId => $calculation) {
            $calculation->requeue();
            $this->removeCalculation($requestId);
        }
    }

    public function removeCalculation(string $requestId): void
    {
        unset($this->calculations[$requestId]);
    }

    private function getCalculation(ChunkRequest $chunkRequest): Calculation
    {
        $calculationRequest = $chunkRequest->getCalculationRequest();
        $requestId = $calculationRequest->getRequestId();
        if (!isset($this->calculations[$requestId])) {
            $this->calculations[$requestId] = new Calculation($this->resultHandler, $calculationRequest);
        }
        return $this->calculations[$requestId];
    }
}
