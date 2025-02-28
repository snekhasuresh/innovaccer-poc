<?php
include_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/breadcrumb.php';

function userpage_breadcrumb_header_shortcode($atts)
{
    // import css
    wp_enqueue_style('breadcrumb-header-style', get_stylesheet_directory_uri() . '/widget-shortcodes/common/usermenu-top/usermenu-top.css');
    $atts = shortcode_atts(
        [
            'iswhite' => 'no',
            'title' => 'My Account',
            'breadcrumb' => 'Home > My Account',
        ],
        $atts
    );

    ob_start();
?>
    <div class="userpage-header">
        <div>
            <?php
            if ($atts['iswhite'] == 'no') {
                echo do_shortcode('[breadcrumb]');
            } else {
                echo do_shortcode('[breadcrumb iswhite="yes"]');
            }
            ?>
        </div>
        <h1 class="page-title"><?php echo $atts['title']; ?></h1>
    </div>
<?php
    return ob_get_clean();
}
add_shortcode('breadcrumb_header', 'userpage_breadcrumb_header_shortcode');
