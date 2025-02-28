<?php
function login_popup_enqueue_scripts()
{
    wp_enqueue_style('login-popup-style', get_stylesheet_directory_uri() . '/widget-shortcodes/common/login-popup/login-popup.css');
    wp_enqueue_script('login-popup-script', get_stylesheet_directory_uri() . '/widget-shortcodes/common/login-popup/login-popup.js', array('jquery'), '1.0', true);

    // Add Google OAuth Script
    wp_enqueue_script('google-platform', 'https://accounts.google.com/gsi/client', array(), null, true);

    // Add AJAX URL, nonce, and Google Client ID
    wp_localize_script('login-popup-script', 'ajax_object', array(
        'ajax_url' => admin_url('admin-ajax.php'),
        'google_client_id' => GOOGLE_CLIENT_ID,
        'google_signin_nonce' => wp_create_nonce('google_signin_nonce')
    ));
}
add_action('wp_enqueue_scripts', 'login_popup_enqueue_scripts');


function login_popup_shortcode()
{
    wp_enqueue_style('user-menu-css', get_stylesheet_directory_uri() . '/widget-shortcodes/common/user-menu/user-menu.css');
    wp_enqueue_script('user-menu-js', get_stylesheet_directory_uri() . '/widget-shortcodes/common/user-menu/user-menu.js');

    ob_start();
    $google_client_id = GOOGLE_CLIENT_ID;
    $token = isset($_COOKIE["wapcar_token"]) ? $_COOKIE["wapcar_token"] : null;

    if (empty($token)) {
        $token = '';
    }
    $response = validate_jwt_token($token);
	error_log('token validation response.....' . $response);
    if ($response != false) {
        include_once(ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/user-menu/user-menu.php');

        echo do_shortcode('[user_dropdown]');
        return ob_get_clean();
    }
?>
    <div class="login-trigger-button">
        <button onclick="openLoginPopup()">Sign Up / Login</button>
    </div>

    <div id="login-popup" class="login-popup">
        <div class="login-popup-content">
            <span class="close-button" onclick="closeLoginPopup()">&times;</span>

            <h2>Sign Up / Login</h2>

            <form id="login-form" onsubmit="handleSubmit(event)">
                <!--                 <div class="form-group">
                    <label for="phone">Phone *</label>
                    <div class="phone-input">
                        <span class="country-code">+60</span>
                        <input type="tel" id="phone" placeholder="Enter Your Phone" required>
                    </div>
                </div>

                <div class="form-group">
                    <label for="otp">OTP *</label>
                    <div class="otp-input">
                        <input type="text" id="otp" placeholder="Enter OTP">
                        <button type="button" onclick="requestOTP()">Request OTP</button>
                    </div>
                </div>

                <button type="submit" class="continue-btn">Continue</button>

                <div class="separator">
                    <span>OR</span>
                </div> -->

                <!--                 <div class="social-login"> -->
                <!--                     <button type="button" class="facebook-btn">
                        <img src="facebook-icon.png" alt="Facebook">
                    </button> -->
                <?php //echo do_shortcode('[nextend_social_login provider="facebook"]'); 
                ?>


                <div id="google-signin-button" style="margin-bottom: 20px"></div>
                <!--                 </div> -->

                <p class="terms">
                    By continuing you indicate that you have read and agree to Wap Car's
                    <a href="<?php echo home_url('/user-agreement'); ?>">User Agreement</a> and <a href="<?php echo home_url('/privacy-policy'); ?>">Privacy Policy</a>
                </p>
            </form>
        </div>
    </div>
<?php
    return ob_get_clean();
}
add_shortcode('login_popup', 'login_popup_shortcode');

function handle_google_signin()
{
    // Verify nonce
    check_ajax_referer('google_signin_nonce', 'nonce');

    $credential = isset($_POST['credential']) ? $_POST['credential'] : '';

    if (empty($credential)) {
        wp_send_json_error('No credential provided');
        return;
    }

    // Load Google Client Library if not already loaded
    if (!class_exists('Google_Client')) {
        require_once ABSPATH . 'vendor/autoload.php';  // Ensure you have installed google/apiclient via Composer
    }

    try {
        $client = new Google_Client(['client_id' => GOOGLE_CLIENT_ID]);
        $payload = $client->verifyIdToken($credential);

        if ($payload) {
            $google_id = $payload['sub'];
            $email = $payload['email'];
            $name = isset($payload['name']) ? $payload['name'] : '';
            $given_name = isset($payload['given_name']) ? $payload['given_name'] : '';
            $family_name = isset($payload['family_name']) ? $payload['family_name'] : '';

            // Check if user exists
            $user = get_user_by('email', $email);

            if (!$user) {
                // Create new user
                $username = sanitize_user(strtolower(str_replace(' ', '_', $name)) . '_' . substr($google_id, 0, 6));
                $random_password = wp_generate_password(12, false);

                $user_id = wp_create_user($username, $random_password, $email);

                if (is_wp_error($user_id)) {
                    wp_send_json_error('Failed to create user: ' . $user_id->get_error_message());
                    return;
                }

                // Update user meta
                update_user_meta($user_id, 'google_id', $google_id);
                update_user_meta($user_id, 'first_name', $given_name);
                update_user_meta($user_id, 'last_name', $family_name);

                wp_update_user(array(
                    'ID' => $user_id,
                    'display_name' => $name
                ));
            } else {
                $user_id = $user->ID;
                update_user_meta($user_id, 'google_id', $google_id);
            }

            // Generate JWT token
            $token = generate_jwt_token($user_id, $email, $name);

            wp_send_json_success(array(
                'token' => $token,
                'message' => 'Login successful'
            ));
            return;
        }

        wp_send_json_error('Invalid token payload');
    } catch (Exception $e) {
        wp_send_json_error('Authentication failed: ' . $e->getMessage());
    }
}
add_action('wp_ajax_nopriv_handle_google_signin', 'handle_google_signin');
add_action('wp_ajax_handle_google_signin', 'handle_google_signin');

// In your functions.php or plugin file
function get_google_redirect_url()
{
    return admin_url('admin-ajax.php');
}

// Update the configuration
wp_localize_script('login-popup-script', 'ajax_object', array(
    'ajax_url' => admin_url('admin-ajax.php'),
    'google_client_id' => GOOGLE_CLIENT_ID,
    'redirect_uri' => get_google_redirect_url()
));



use Twilio\Rest\Client;

// Handle OTP request
function request_otp_handler()
{
    $phone_number = sanitize_text_field($_POST['phone']);

    if (empty($phone_number)) {
        wp_send_json_error(['message' => 'Phone number is required']);
    }

    $user = get_user_by_phone($phone_number);
    if (!$user) {
        wp_send_json_error(['message' => 'Phone number is not registered']);
    }

    $otp = rand(100000, 999999);
    set_transient('otp_' . $phone_number, $otp, 300);

    // for testing purposes, uncomment the following line
    // wp_send_json_success(['message' => 'OTP sent successfully', 'phone_number' => $phone_number, 'otp' => $otp]);

    if (!defined('TWILIO_SID') || !defined('TWILIO_TOKEN') || !defined('TWILIO_PHONE_NUMBER')) {
        return 'Twilio credentials are missing.';
    }

    $sid = TWILIO_SID;
    $token = TWILIO_TOKEN;
    $twilio_number = TWILIO_PHONE_NUMBER;
    $twilio = new Twilio\Rest\Client($sid, $token);

    try {
        $message = $twilio->messages->create(
            $phone_number, // Phone number to send the message to
            [
                'from' => $twilio_number,
                'body' => 'Your OTP is: ' . $otp
            ]
        );
        wp_send_json_success(['message' => 'OTP sent successfully']);
    } catch (Exception $e) {
        wp_send_json_error(['message' => 'Failed to send OTP. Error: ' . $e->getMessage()]);
    }
}
add_action('wp_ajax_request_otp', 'request_otp_handler');
add_action('wp_ajax_nopriv_request_otp', 'request_otp_handler');


use Firebase\JWT\JWT;

function verify_login_handler()
{
    $phone_number = sanitize_text_field($_POST['phone']);
    $otp = sanitize_text_field($_POST['otp']);

    if (empty($phone_number) || empty($otp)) {
        wp_send_json_error(['message' => 'Phone number and OTP are required']);
    }

    // Retrieve the OTP from the transient
    $stored_otp = get_transient('otp_' . $phone_number);

    if ($stored_otp && $stored_otp == $otp) {
        // OTP is valid
        delete_transient('otp_' . $phone_number);

        // Generate JWT token
        $user = get_user_by_phone($phone_number);
        if (!$user) {
            wp_send_json_error(['message' => 'Phone number is not registered']);
        }

        $key = AUTH_KEY;
        $issuedAt = time();
        $expire = $issuedAt + (60 * 60 * 24);
        $payload = [
            'iss' => get_site_url(),
            'iat' => $issuedAt,
            'exp' => $expire,
            'user_id' => $user->ID,
            'email' => $user->user_email,
            'name' => $user->display_name
        ];
        $token = JWT::encode($payload, $key, 'HS256');

        wp_send_json_success(['message' => 'OTP verified successfully', 'token' => $token]);
    } else {
        // OTP is invalid or expired
        wp_send_json_error(['message' => 'Invalid or expired OTP']);
    }
}
add_action('wp_ajax_verify_login', 'verify_login_handler');
add_action('wp_ajax_nopriv_verify_login', 'verify_login_handler');

function get_user_by_phone($phone)
{
    $users = get_users(array(
        'meta_key'   => 'phone',
        'meta_value' => $phone,
        'number'     => 1,
        'count_total' => false,
    ));

    return !empty($users) ? $users[0] : false;
}

function generate_jwt_token($user_id, $email, $name)
{
    $key = AUTH_KEY;
    $issuedAt = time();
    $expire = $issuedAt + (60 * 60 * 24);
    $payload = [
        'iss' => get_site_url(),
        'iat' => $issuedAt,
        'exp' => $expire,
        'user_id' => $user_id,
        'email' => $email,
        'name' => $name
    ];
    return JWT::encode($payload, $key, 'HS256');
}


function user_dropdown_menu_shortcode($atts)
{
    ob_start();

    if (isset($_GET['action']) && $_GET['action'] === 'logout') {
        if (isset($_COOKIE['wapcar_token'])) {
            unset($_COOKIE['wapcar_token']);
            setcookie('wapcar_token', '', time() - 3600, '/');
            wp_redirect(home_url());
            exit();
        }
    }

    $token = isset($_COOKIE["wapcar_token"]) ? $_COOKIE["wapcar_token"] : '';
	error_log('token value.........' . $token);
    $response = validate_jwt_token($token);
    $user_name = $response['name'];
    $first_letter = ucfirst(substr($user_name, 0, 1));
?>


    <div class="user-dropdown">
        <div class="user-dropdown-toggle"><?php echo esc_html($first_letter); ?></div>
        <div class="user-dropdown-menu">

            <a href="<?php echo esc_url(home_url('/car-owner-service')); ?>" class="user-dropdown-item">
                <i class="fas fa-car"></i> My Car
            </a>
            <span onclick="logout()" class="user-dropdown-item">
                <i class="fas fa-sign-out-alt"></i> Log Out
            </span>
        </div>
    </div>

    <script>
        function logout() {
            document.cookie = "wapcar_token=; path=/; expires=Thu, 01 Jan 1970 00:00:00 UTC;";
            // window.location.reload();
            window.location.href = '/';

        }
    </script>

    <style>
        .user-dropdown {
            position: relative;
            display: inline-block;
        }

        .user-dropdown-toggle {
            background: #ff5733;
            color: white;
            width: 40px;
            height: 40px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            font-weight: bold;
        }

        .user-dropdown-menu {
            display: none;
            position: absolute;
            /* right: 0; */
            left: 100%;
            top: 0;
            background-color: white;
            min-width: 160px;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.2);
            border-radius: 4px;
            padding: 8px 0;
            z-index: 1000;
        }

        .user-dropdown:hover .user-dropdown-menu {
            display: block;
        }

        .user-dropdown-item {
            display: flex;
            align-items: center;
            padding: 8px 16px;
            text-decoration: none;
            color: #333;
            cursor: pointer;
        }

        .user-dropdown-item:hover {
            background-color: #f5f5f5;
        }

        .user-dropdown-item i {
            margin-right: 8px;
            width: 20px;
        }
    </style>
<?php

    return ob_get_clean();
}

add_shortcode('user_dropdown', 'user_dropdown_menu_shortcode');


