<?php

use Firebase\JWT\JWT;
use Firebase\JWT\Key;

function validate_jwt_token($token)
{
    $secret_key = AUTH_KEY;

    try {
        $decoded = JWT::decode($token, new Key($secret_key, 'HS256'));
		error_log('Token validate succesfully..........');
        return (array) $decoded;
    } catch (Exception $e) {
        error_log('JWT Validation Error: ' . $e->getMessage());
        return false;
    }
}
