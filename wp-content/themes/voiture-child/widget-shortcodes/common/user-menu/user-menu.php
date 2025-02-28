<?php

function user_menu_shortcode()
{
    wp_enqueue_style('user-menu-css', get_stylesheet_directory_uri() . '/widget-shortcodes/common/user-menu/user-menu.css');
    wp_enqueue_script('user-menu-js', get_stylesheet_directory_uri() . '/widget-shortcodes/common/user-menu/user-menu.js');


    ob_start();

    if (is_user_logged_in()) {
        $home_url = home_url();
        $logout_url = wp_logout_url(home_url());

        $current_user = wp_get_current_user();
        $user_name = $current_user->display_name;
        $user_first_letter = ucfirst(substr($user_name, 0, 1));
?>
        <div class="user-dropdown">
            <button class="dropdown-toggle">
                <span class="user-initial"><?php echo $user_first_letter; ?></span>
                <i class="dropdown-icon">▼</i>
            </button>
            <ul class="dropdown-menu">
                <li><a href="<?php echo esc_url($home_url . '/me/history'); ?>"><i class="menu-icon clock"></i> History</a></li>
                <li><a href="<?php echo esc_url($home_url . '/me/favourite'); ?>"><i class="menu-icon bookmark"></i> Favorite</a></li>
                <li><a href="<?php echo esc_url($home_url . '/car-owner-service'); ?>"><i class="menu-icon car"></i> My Car</a></li>
                <li><a href="<?php echo esc_url($logout_url); ?>"><i class="menu-icon power"></i> Log Out</a></li>
            </ul>
        </div>
    <?php
    } else {
        // If user is not logged in, show login/signup button
        $login_url = home_url('/wp-login.php?redirect_to=' . urlencode(home_url()));
    ?>
        <a href="<?php echo esc_url($login_url); ?>" class="login-signup-button">Login/Signup</a>
<?php
    }

    return ob_get_clean();
}
add_shortcode('user_menu', 'user_menu_shortcode');
