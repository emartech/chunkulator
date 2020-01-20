<?php

namespace Emartech\Chunkulator\Calculator;

interface Filter
{
    public function filterContacts(array $requestData, array $contactIds): array;
}
