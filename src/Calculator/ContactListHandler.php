<?php

namespace Emartech\Chunkulator\Calculator;

interface ContactListHandler
{
    public function getContactsOfList(array $requestData, int $limit, int $offset): array;
    public function applyContactsToList(array $requestData, array $contactIds): void;
}
