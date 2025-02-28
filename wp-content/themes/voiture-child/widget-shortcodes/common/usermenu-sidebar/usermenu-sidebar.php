<?php
function user_sidebar_shortcode()
{
    wp_enqueue_style('custom-sidebar-style', get_stylesheet_directory_uri() . '/widget-shortcodes/common/usermenu-sidebar/usermenu-sidebar.css');

    $token = $_COOKIE["wapcar_token"];
    if (empty($token)) {
        $token = '';
    }
    $response = validate_jwt_token($token);
    if ($response == false) {
        return '';
    }
    $user_name = $response['name'];
    $user_email = $response['email'];

    $home_url = home_url();
    $links = [
        'My Car' => $home_url . '/car-owner-service',
    ];

    ob_start();
?>

    <div class="custom-sidebar">
        <div class="user-profile">
            <!-- Display user avatar -->
            <div class="user-avatar">
                <span><?php echo strtoupper(substr($user_name, 0, 1)); ?></span>
            </div>
            <!-- Display user name and email -->
            <div class="user-info">
                <h4><?php echo esc_html($user_name); ?></h4>
                <p><?php echo esc_html($user_email); ?></p>
            </div>
        </div>
        <ul class="sidebar-links">
            <?php foreach ($links as $label => $url): ?>
                <li>
                    <a href="<?php echo esc_url($url); ?>" class="<?php echo is_page($label) ? 'active' : ''; ?>">
                        <?php echo esc_html($label); ?>
                    </a>
                </li>
            <?php endforeach; ?>
        </ul>
    </div>

<?php
    return ob_get_clean();
}
add_shortcode('user_sidebar', 'user_sidebar_shortcode');
