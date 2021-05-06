<?php

namespace Emartech\Chunkulator\Notifier;

use Emartech\Chunkulator\ResourceFactory as ResourceFactoryInterface;
use Emartech\Chunkulator\Notifier\Consumer as Processor;
use Psr\Log\LoggerInterface;

class Worker
{
    private $resourceFactory;


    public function __construct(ResourceFactoryInterface $resourceFactory)
    {
        $this->resourceFactory = $resourceFactory;
    }

    public function run(LoggerInterface $logger): void
    {
        $logger->debug('run start');
        $queueFactory = $this->resourceFactory->createQueueFactory();
        $context = $queueFactory->createContext();
        $queue = $queueFactory->createNotifierQueue($context);
        $consumer = $context->createConsumer($queue);

        $processor = new Processor($this->resourceFactory->createResultHandler(), $logger, $queueFactory);

        do {
            $message = $consumer->receive($queueFactory->getConnectionTimeOut());
            $logger->debug('message received', [
                'message' => $message
            ]);

            if ($message) {
                $processor->consume($consumer, $message);
            } else {
                $processor->timeOut($consumer);
            }
        }
        while ($message);
        $logger->debug('run end');
    }
}
