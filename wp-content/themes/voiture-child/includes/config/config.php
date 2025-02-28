<?php
// Constants for cache timeout
define('REDIS_CACHE_TIMEOUT', 3600);  // Redis cache timeout
define('TRANSIENT_CACHE_TIMEOUT', 3600); // Transient cache timeout

// Flags to toggle caching strategies
define('USE_REDIS_CACHE', true); // Use Redis for caching
define('USE_TRANSIENT_CACHE', false); // Use WordPress Transients for caching
define('FETCH_FROM_DB', false); // Use wisely
define('REDIS_DEFAULT_TIMEOUT', 0);
define('CAR_PLACEHOLDER', 'https://images.wapcar.my/file1/88aaa06bdb554fb18c5b0e73651997ab_606x402.jpg');
