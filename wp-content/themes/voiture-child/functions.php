<?php

/**
 * voiture Child functions and definitions
 *
 * Set up the theme and provides some helper functions, which are used in the
 * theme as custom template tags. Others are attached to action and filter
 * hooks in WordPress to change core functionality.
 *
 * When using a child theme you can override certain functions (those wrapped
 * in a function_exists() call) by defining them first in your child theme's
 * functions.php file. The child theme's functions.php file is included before
 * the parent theme's file, so the child theme functions would be used.
 *
 * @link https://codex.wordpress.org/Theme_Development
 * @link https://codex.wordpress.org/Child_Themes
 *
 * Functions that are not pluggable (not wrapped in function_exists()) are
 * instead attached to a filter or action hook.
 *
 * For more information on hooks, actions, and filters,
 * {@link https://codex.wordpress.org/Plugin_API}
 *
 * @package WordPress
 * @subpackage Voiture
 * @since Voiture Child Theme
 */
wp_enqueue_style('fontawesome', 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css', [], '6.5.1');
function remove_posts_menu() {
    remove_menu_page('edit.php'); // Hides "Posts" from the admin menu
}
add_action('admin_menu', 'remove_posts_menu');

function load_fontawesome()
{
    wp_enqueue_style('fontawesome', get_stylesheet_directory_uri() . '/css/font-awesome.css', array(), '6.5.1');
}
add_action('wp_enqueue_scripts', 'load_fontawesome');

function override_hfe_blockquote_css()
{
    // Remove any CSS file that the plugin might have added
    wp_dequeue_style('hfe-widget-blockquote');

    // Ensure we only load the correct CSS when Pro Elements is active
    if (defined('PRO_ELEMENTS_VERSION')) {
        wp_enqueue_style(
            'hfe-widget-blockquote',
            plugin_dir_url(WP_PLUGIN_DIR . '/pro-elements/pro-elements.php') . 'assets/css/widget-blockquote.min.css',
            [],
            '3.25.0'
        );
    }
}
add_action('wp_enqueue_scripts', 'override_hfe_blockquote_css', 30);

function enqueue_pro_element_custom_styles()
{
    // Check if we are on the front-end and the 'pro-elements' plugin is active
    if (! is_admin() && is_plugin_active('pro-elements/pro-elements.php')) {

        // Dequeue Elementor Pro's widget-mega-menu.min.css
        wp_dequeue_style('elementor-widget-mega-menu'); // Ensure this is the correct handle for Elementor Pro's mega-menu style

        // Enqueue widget-nav-menu.min.css from the pro-elements plugin folder
        wp_enqueue_style('widget-nav-menu', plugin_dir_url(WP_PLUGIN_DIR . '/pro-elements/pro-elements.php') . 'assets/css/widget-nav-menu.min.css', array(), null, 'all');

        // Enqueue widget-mega-menu.min.css from the pro-elements plugin folder
        wp_enqueue_style('widget-mega-menu', plugin_dir_url(WP_PLUGIN_DIR . '/pro-elements/pro-elements.php') . 'assets/css/widget-mega-menu.min.css', array(), null, 'all');
    }
}
add_action('wp_enqueue_scripts', 'enqueue_pro_element_custom_styles');

function enqueue_fontawesome()
{
    wp_enqueue_style('fontawesome', 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css');
}
add_action('wp_enqueue_scripts', 'enqueue_fontawesome');


function disable_heartbeat_for_voiture()
{
    wp_deregister_script('heartbeat');
}
add_action('init', 'disable_heartbeat_for_voiture');

function disable_voiture_social_login()
{
    remove_action('init', 'wp_cardealer_social_facebook'); // Facebook login
    remove_action('init', 'wp_cardealer_social_google'); // Google login (if exists)
    remove_action('login_form', 'wp_cardealer_social_facebook'); // Login form social buttons
}
add_action('init', 'disable_voiture_social_login', 1);

function custom_prevent_duplicate_sessions()
{
    if (session_status() == PHP_SESSION_NONE) {
        // session_start();
    }
}
add_action('init', 'custom_prevent_duplicate_sessions', 1);

function voiture_child_enqueue_styles()
{
    // Enqueue parent style
    wp_enqueue_style('voiture-parent-style', get_template_directory_uri() . '/style.css');

    // Enqueue child style
    wp_enqueue_style('voiture-child-style', get_stylesheet_directory_uri() . '/style.css', array('voiture-parent-style'));
}
add_action('wp_enqueue_scripts', 'voiture_child_enqueue_styles');
// End 

//function disable_wpcd_social_login() {
//   remove_action('wp_ajax_wp_cardealer_facebook_login', 'wp_cardealer_social_facebook');
//  remove_action('wp_ajax_nopriv_wp_cardealer_facebook_login', 'wp_cardealer_social_facebook');
//}
//add_action('init', 'disable_wpcd_social_login', 1);

function remove_all_canonical_links()
{
    remove_action('wp_head', 'rel_canonical');
    add_filter('wpseo_canonical', '__return_false');
}
add_action('init', 'remove_all_canonical_links');

function get_current_language()
{
    $language_map = [
        'English' => '',
        'Bahasa Malaysia' => 'my-my',
        '中文' => 'my-zh'
    ];
    $selectedLanguage = isset($_COOKIE['preferred_language']) ? $_COOKIE['preferred_language'] : 'English';
    return $language_map[$selectedLanguage];
}

// declare global variable $ad_unit
$ad_unit = '<!-- ad unit -->
<script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-8521211126902149" crossorigin="anonymous"></script>
<ins class="adsbygoogle"
    style="display:block"
    data-ad-client="ca-pub-8521211126902149"
    data-ad-slot="7561364664"
    data-ad-format="auto"
    data-full-width-responsive="true"></ins>
<script>
    (adsbygoogle = window.adsbygoogle || []).push({});
</script>';

function get_ins_ad_unit()
{
    global $ad_unit;
    return $ad_unit;
}

// Declaring listing_make_evs taxonomy 
function register_listing_make_evc_taxonomy_duplicate()
{
    register_taxonomy('listing_make_evc', array('listing'), array(
        'label' => __('Listing Make EVC'),
        'rewrite' => array('slug' => 'listing-make-evc'),
        'hierarchical' => true,
        'show_admin_column' => true, // Shows taxonomy in the admin post list
    ));
}
add_action('init', 'register_listing_make_evc_taxonomy_duplicate');

// Include Configuration Constants
require_once get_stylesheet_directory() . '/includes/config/config.php';
// Include Redis Constants
require_once get_stylesheet_directory() . '/includes/redis-helper/redis-operations.php';
// Adding Cache Management
require_once get_stylesheet_directory() . '/includes/query-helper/common/helper_class.php';
// Include All Query files
require_once get_stylesheet_directory() . '/includes/query-helper/loader.php';
// Include Cache Management Functions
require_once get_stylesheet_directory() . '/includes/cache-helper/cache-manager.php';
// Include Admin Hooks for Post Save/Delete
require_once get_stylesheet_directory() . '/includes/admin-hooks/clear-cache.php';


//motor article page
require_once ABSPATH . 'wp-content/themes/voiture-child/motor-article-page.php';

function add_gpt_script_to_head()
{
?>
    <script async src='https://securepubads.g.doubleclick.net/tag/js/gpt.js'></script>
    <script>
        window.googletag = window.googletag || {
            cmd: []
        };




        function isMobileDevice() {
            return /android|webos|iphone|ipad|ipod|blackberry|iemobile|opera mini/i.test(navigator.userAgent);
        }




        googletag.cmd.push(function() {
            const urlPath = window.location.pathname;
            const path = window.location.pathname;
            const make = "<?php echo get_query_var('make'); ?>";
            const model = "<?php echo get_query_var('model'); ?>";
            const section = "<?php echo get_query_var('section'); ?>";
            const variantSection = "<?php echo get_query_var('variant_section'); ?>";
            const newsSlug = "<?php echo get_query_var('news_slug'); ?>";
            const currentYear = new Date().getFullYear();
            const sections = ['overview', 'news', 'specs', 'gallery', 'fuel-consumption', 'colors'];




            console.log({
                newsSlug
            });




            if (!isMobileDevice()) {
                // home
                if (urlPath == '/' || urlPath == '') {
                    googletag.defineSlot('/22557728108/vn_hp_latestnews_above_pc', [728, 90], 'div-gpt-ad-1740632169770-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_hp_leaderboard_under_pc', [728, 90], 'div-gpt-ad-1740632080264-0').addService(googletag.pubads());
                }




                // news      
                if ((urlPath.includes('/news') && !newsSlug) || urlPath === '/bm' || urlPath === '/zh') {
                    googletag.defineSlot('/22557728108/vn_news_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740632194730-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_news_sidebar_end_pc', [300, 250], 'div-gpt-ad-1740632271021-0').addService(googletag.pubads());
                }




                // news individual
                if (urlPath.includes('/news') && newsSlug) {
                    const categories = ['latest', 'reviews', 'opinions', 'evs', 'buying-guides', 'owner-stories', 'used-car'];
                    if (!categories.includes(newsSlug)) {
                        googletag.defineSlot('/22557728108/th_article_fourthp_under_pc', [728, 90], 'div-gpt-ad-1735654226361-0').addService(googletag.pubads());
                        googletag.defineSlot('/22557728108/vn_article_relatedmodel_above_pc', [728, 90], 'div-gpt-ad-1740632337418-0').addService(googletag.pubads());
                        googletag.defineSlot('/22557728108/vn_article_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740632363274-0').addService(googletag.pubads());
                    } else {
                        googletag.defineSlot('/22557728108/vn_news_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740632194730-0').addService(googletag.pubads());
                        googletag.defineSlot('/22557728108/vn_news_sidebar_end_pc', [300, 250], 'div-gpt-ad-1740632271021-0').addService(googletag.pubads());
                    }
                }








                // cars
                if (urlPath === '/cars') {
                    googletag.defineSlot('/22557728108/vn_cars_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740633276982-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_cars_popularbrand_above_pc', [728, 90], 'div-gpt-ad-1740633403925-0').addService(googletag.pubads());
                }




                // cars - brand
                if (make && !model) {
                    googletag.defineSlot('/22557728108/my_brand_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740554034992-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_brand_modellist_under_pc', [728, 90], 'div-gpt-ad-1740632526161-0').addService(googletag.pubads());
                }




                if (make && model && !section) {
                    googletag.defineSlot('/22557728108/vn_model_usedcar_under_pc', [728, 90], 'div-gpt-ad-1740632392311-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_model_sidebar_1_pc', [300, 250], 'div-gpt-ad-1740632439887-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_model_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740632468198-0').addService(googletag.pubads());
                }




                if (make && model && section && sections.includes(section)) {
                    switch (section) {
                        case 'overview':
                            googletag.defineSlot('/22557728108/vn_model_usedcar_under_pc', [728, 90], 'div-gpt-ad-1740632392311-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/vn_model_sidebar_1_pc', [300, 250], 'div-gpt-ad-1740632439887-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/vn_model_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740632468198-0').addService(googletag.pubads());
                            break;




                        case 'news':
                            break;




                        case 'specs':
                            break;




                        case 'gallery':
                            googletag.defineSlot('/22557728108/VN_ModelImages_FirstScreen_LeftSide_PC', [160, 600], 'div-gpt-ad-1740632678344-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/VN_ModelImages_FirstScreen_RightSide_PC', [160, 600], 'div-gpt-ad-1740632702902-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/vn_modelimages_sidebar_2_pc', [300, 250], 'div-gpt-ad-1740632729238-0').addService(googletag.pubads());
                            break;




                        case 'fuel-consumption':
                            googletag.defineSlot('/22557728108/vn_fuelconsumption_firstscreen_leftside_pc', [160, 600], 'div-gpt-ad-1740632846830-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/vn_fuelconsumption_firstscreen_rightside_pc', [160, 600], 'div-gpt-ad-1740632878007-0').addService(googletag.pubads());
                            break;




                        case 'colors':
                            googletag.defineSlot('/22557728108/vn_colors_firstscreen_leftsides_pc', [160, 600], 'div-gpt-ad-1740632757226-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/vn_colors_firstscreen_rightsides_pc', [160, 600], 'div-gpt-ad-1740632784137-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/vn_colors_sidebar_end_pc', [300, 250], 'div-gpt-ad-1740632822287-0').addService(googletag.pubads());
                            break;




                        default:
                            break;
                    }
                }




                if (make && model && section && !sections.includes(section)) {
                    googletag.defineSlot('/22557728108/vn_variant_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740632931062-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_variant_sidebar_2_pc', [300, 250], 'div-gpt-ad-1740632952583-0').addService(googletag.pubads());
                }




                if (make && model && section && variantSection) {
                    switch (variantSection) {
                        case 'overview':
                            googletag.defineSlot('/22557728108/vn_variant_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740632931062-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/vn_variant_sidebar_2_pc', [300, 250], 'div-gpt-ad-1740632952583-0').addService(googletag.pubads());
                            break;




                        case 'news':
                            break;




                        case 'specs':
                            break;




                        case 'gallery':
                            googletag.defineSlot('/22557728108/my_vairiantimages_firstscreen_rightside_pc', [160, 600], 'div-gpt-ad-1735656697579-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/my_vairiantimages_sidebar_2_pc', [300, 250], 'div-gpt-ad-1735656715250-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/my_vairiantimages_firstscreen_leftside_pc', [160, 600], 'div-gpt-ad-1735656732530-0').addService(googletag.pubads());
                            break;




                        case 'fuel-consumption':
                            break;




                        case 'colors':
                            break;




                        default:
                            break;
                    }
                }




                // Tools
                if (urlPath === '/tools/loan-calculator') {
                    googletag.defineSlot('/22557728108/vn_loantool_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740633057034-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_loantool_sidebar_end_pc', [300, 250], 'div-gpt-ad-1740633078701-0').addService(googletag.pubads());
                }




                if (urlPath === '/tools/insurance-calculator') {
                    googletag.defineSlot('/22557728108/vn_insurancetool_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740633109519-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_insurancetool_sidebar_end_pc', [300, 250], 'div-gpt-ad-1740633133048-0').addService(googletag.pubads());
                }




                if (urlPath === '/tools/road-tax-calculator') {
                    googletag.defineSlot('/22557728108/my_taxtool_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1735652450620-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/my_taxtool_sidebar_end_pc', [300, 250], 'div-gpt-ad-1735652611596-0').addService(googletag.pubads());
                }




                if (urlPath === '/tools/fuel-cost-calculator') {
                    googletag.defineSlot('/22557728108/my_fuelcosttool_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1735652711805-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/my_fuelcosttool_sidebar_end_pc', [300, 250], 'div-gpt-ad-1735652767968-0').addService(googletag.pubads());
                }




                if (urlPath.includes('/compare-cars')) {
                    googletag.defineSlot('/22557728108/vn_compare_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740632581185-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/my_compare_smyebar_1_pc', [300, 250], 'div-gpt-ad-1735655162916-0').addService(googletag.pubads());




                    // googletag.defineSlot('/22557728108/vn_compareresult_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740632627224-0').addService(googletag.pubads());
                    // googletag.defineSlot('/22557728108/my_compareresult_smyebar_1_pc', [300, 250], 'div-gpt-ad-1735655347796-0').addService(googletag.pubads());
                }




                if (urlPath === '/fuel-price') {
                    googletag.defineSlot('/22557728108/vn_fuelpricetool_breadcrumb_above_pc', [728, 90], 'div-gpt-ad-1740633212691-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_fuelpricetool_sidebar_end_pc', [300, 250], 'div-gpt-ad-1740633245602-0').addService(googletag.pubads());
                }
            } else {
                // mobile
                // home
                if (urlPath == '/' || urlPath == '') {
                    googletag.defineSlot('/22557728108/vn_hp_feed_1_wap', [300, 250], 'div-gpt-ad-1740573382277-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/th_hp_feed_4_wap', [336, 280], 'div-gpt-ad-1740550577623-0').addService(googletag.pubads());
                }




                // article page
                if (newsSlug) {
                    const categories = ['latest', 'reviews', 'opinions', 'evs', 'buying-guides', 'owner-stories', 'used-car'];




                    if (!categories.includes(newsSlug)) {
                        googletag.defineSlot('/22557728108/vn_article_fourthp_under_wap', [300, 250], 'div-gpt-ad-1740573447077-0').addService(googletag.pubads());
                        googletag.defineSlot('/22557728108/vn_article_rating_under_wap', [336, 280], 'div-gpt-ad-1740573472221-0').addService(googletag.pubads());
                    }
                }




                // car brand page
                if (make && !model) {
                    googletag.defineSlot('/22557728108/vn_brand_usedcar_above_wap', [300, 250], 'div-gpt-ad-1740573598877-0').addService(googletag.pubads());
                }




                // model overview page
                if (make && model && !section) {
                    googletag.defineSlot('/22557728108/vn_model_variantlist_above_wap', [320, 100], 'div-gpt-ad-1740573497257-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_model_variantlist_under_wap', [300, 250], 'div-gpt-ad-1740573524077-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_model_gallery_under_wap', [336, 280], 'div-gpt-ad-1740573567656-0').addService(googletag.pubads());
                }




                // model faq page
                // googletag.defineSlot('/22557728108/my_faqs_relatedmodel_above_wap', [300, 250], 'div-gpt-ad-1736085201790-0').addService(googletag.pubads());
                // googletag.defineSlot('/22557728108/th_faqs_carimages_under_wap', [336, 280], 'div-gpt-ad-1740552372575-0').addService(googletag.pubads());




                if (make && model && section && sections.includes(section)) {
                    switch (section) {
                        case 'overview':
                            break;




                        case 'gallery':
                            // model images page
                            googletag.defineSlot('/22557728108/vn_modelimages_relatedmodel_above_wap', [300, 250], 'div-gpt-ad-1740573668080-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/vn_modelimages_carimages_under_wap', [336, 280], 'div-gpt-ad-1740573691977-0').addService(googletag.pubads());
                            break;




                        case 'fuel-consumption':
                            // model fuel consumption page
                            googletag.defineSlot('/22557728108/vn_fuelconsumption_othervariant_above_wap', [300, 250], 'div-gpt-ad-1740573768696-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/vn_fuelconsumption_comparison_above_wap', [336, 280], 'div-gpt-ad-1740573794429-0').addService(googletag.pubads());
                            break;




                        case 'colors':
                            // model colors page
                            googletag.defineSlot('/22557728108/vn_colors_relatedmodel_above_wap', [300, 250], 'div-gpt-ad-1740573717941-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/vn_colors_gallery_under_wap', [336, 280], 'div-gpt-ad-1740573741895-0').addService(googletag.pubads());
                            break;




                        default:
                            break
                    }
                }




                // user reviews page
                // googletag.defineSlot('/22557728108/vn_userreviews_relatedmodel_under_m_wap', [300, 250], 'div-gpt-ad-1740573817369-0').addService(googletag.pubads());




                // variant overview page
                if (make && model && section && !sections.includes(section) && !variantSection) {
                    googletag.defineSlot('/22557728108/vn_variant_dealer_above_wap', [300, 250], 'div-gpt-ad-1740573840591-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_variant_carimages_under_wap', [336, 280], 'div-gpt-ad-1740573865368-0').addService(googletag.pubads());
                }




                if (make && model && section && !sections.includes(section) && variantSection) {
                    switch (variantSection) {
                        case 'overview':
                            break;




                        case 'gallery':
                            // variant images page
                            googletag.defineSlot('/22557728108/vn_variantimages_related_above_wap', [300, 250], 'div-gpt-ad-1740573892425-0').addService(googletag.pubads());
                            googletag.defineSlot('/22557728108/vn_variantimages_carimages_under_wap', [336, 280], 'div-gpt-ad-1740573925132-0').addService(googletag.pubads());
                            break;




                        default:
                            break;
                    }
                }




                // Tools
                if (urlPath === '/tools/loan-calculator') {
                    googletag.defineSlot('/22557728108/vn_loantool_moretools_above_wap', [300, 250], 'div-gpt-ad-1740573946847-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_loantool_faq_above_wap', [336, 280], 'div-gpt-ad-1740573972475-0').addService(googletag.pubads());
                }




                if (urlPath === '/tools/insurance-calculator') {
                    googletag.defineSlot('/22557728108/vn_insurancetool_moretools_above_wap', [300, 250], 'div-gpt-ad-1740631656194-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_insurancetool_faq_above_wap', [336, 280], 'div-gpt-ad-1740631680933-0').addService(googletag.pubads());
                }




                if (urlPath === '/tools/road-tax-calculator') {
                    googletag.defineSlot('/22557728108/th_taxtool_moretools_above_wap', [300, 250], 'div-gpt-ad-1740553113403-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/th_taxtool_faq_above_wap', [336, 280], 'div-gpt-ad-1740553166075-0').addService(googletag.pubads());
                }




                if (urlPath === '/tools/fuel-cost-calculator') {
                    googletag.defineSlot('/22557728108/th_fuelcosttool_moretools_above_wap', [300, 250], 'div-gpt-ad-1740553224039-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/th_fuelcosttool_faq_above_wap', [336, 280], 'div-gpt-ad-1740553272331-0').addService(googletag.pubads());
                }




                if (urlPath === '/fuel-price') {
                    googletag.defineSlot('/22557728108/vn_fuelpricetool_moretools_above_wap', [300, 250], 'div-gpt-ad-1740631810447-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/vn_fuelpricetool_faq_above_wap', [336, 280], 'div-gpt-ad-1740631838534-0').addService(googletag.pubads());
                }




                // Cars For Sale
                if (urlPath === '/tools/used-car-market-value-guide') {
                    googletag.defineSlot('/22557728108/my_valuetool_faq_above_wap', [300, 250], 'div-gpt-ad-1736086673824-0').addService(googletag.pubads());
                    googletag.defineSlot('/22557728108/th_tradeintool_popularbrand_above_wap', [300, 250], 'div-gpt-ad-1740553417292-0').addService(googletag.pubads());
                }
            }




            googletag.pubads().enableSingleRequest();
            googletag.enableServices();
        });
    </script>
    <?php
}
add_action('wp_head', 'add_gpt_script_to_head');




$ad_units_mapping = [
    // home
    'VN_Hp_Leaderboard_Under_PC' => ['page' => 'home', 'id' => '1740632080264', 'size' => [728, 90], 'is_mobile' => false],
    'VN_HP_Latestnews_Above_PC' => ['page' => 'home', 'id' => '1740632169770', 'size' => [728, 90], 'is_mobile' => false],




    // news
    '   VN_News_Breadcrumb_Above_PC' => ['page' => 'news', 'id' => '1740632194730', 'size' => [728, 90], 'is_mobile' => false],
    'VN_News_Sidebar_End_PC' => ['page' => 'news', 'id' => '1740632271021', 'size' => [300, 250], 'is_mobile' => false],
    'MY_Article_Fourthp_Under_PC' => ['page' => 'article', 'id' => '1735654226361', 'size' => [728, 90], 'is_mobile' => false],
    'VN_Article_Relatedmodel_Above_PC' => ['page' => 'article', 'id' => '1740632337418', 'size' => [728, 90], 'is_mobile' => false],
    'VN_Article_Breadcrumb_Above_PC' => ['page' => 'article', 'id' => '1740632363274', 'size' => [728, 90], 'is_mobile' => false],




    // cars
    'VN_Cars_Breadcrumb_Above_PC' => ['page' => 'cars', 'id' => '1740633276982', 'size' => [728, 90], 'is_mobile' => false],
    'VN_Cars_Popularbrand_Above_PC' => ['page' => 'cars', 'id' => '1740633403925', 'size' => [728, 90], 'is_mobile' => false],
    'MY_Brand_Breadcrumb_Above_PC' => ['page' => 'cars', 'id' => '1735654872196', 'size' => [728, 90], 'is_mobile' => false],
    'VN_Brand_Modellist_Under_PC' => ['page' => 'cars', 'id' => '1740632526161', 'size' => [728, 90], 'is_mobile' => false],




    // model
    'VN_Model_Usedcar_Under_PC' => ['page' => 'model', 'id' => '1740632392311', 'size' => [728, 90], 'is_mobile' => false],
    'VN_Model_Sidebar_1_PC' => ['page' => 'model', 'id' => '1740632439887', 'size' => [300, 250], 'is_mobile' => false],
    'VN_Model_Breadcrumb_Above_PC' => ['page' => 'model', 'id' => '1740632468198', 'size' => [728, 90], 'is_mobile' => false],
    'VN_ModelImages_FirstScreen_LeftSide_PC' => ['page' => 'model', 'id' => '1740632678344', 'size' => [160, 600], 'is_mobile' => false],
    'VN_ModelImages_FirstScreen_RightSide_PC' => ['page' => 'model', 'id' => '1740632702902', 'size' => [160, 600], 'is_mobile' => false],
    'VN_ModelImages_Sidebar_2_PC' => ['page' => 'model', 'id' => '1740632729238', 'size' => [300, 250], 'is_mobile' => false],
    'VN_Colors_FirstScreen_LeftSides_PC' => ['page' => 'model', 'id' => '1740632757226', 'size' => [160, 600], 'is_mobile' => false],
    'VN_Colors_FirstScreen_RightSides_PC' => ['page' => 'model', 'id' => '1740632784137', 'size' => [160, 600], 'is_mobile' => false],
    'VN_Colors_Sidebar_End_PC' => ['page' => 'model', 'id' => '1740632822287', 'size' => [300, 250], 'is_mobile' => false],
    'VN_Fuelconsumption_FirstScreen_LeftSide_PC' => ['page' => 'model', 'id' => '1740632846830', 'size' => [160, 600], 'is_mobile' => false],
    'VN_Fuelconsumption_FirstScreen_RightSide_PC' => ['page' => 'model', 'id' => '1740632878007', 'size' => [160, 600], 'is_mobile' => false],




    // variant
    'VN_Variant_Breadcrumb_Above_PC' => ['page' => 'variant', 'id' => '1740632931062', 'size' => [728, 90], 'is_mobile' => false],
    'VN_Variant_Sidebar_2_PC' => ['page' => 'variant', 'id' => '1740632952583', 'size' => [300, 250], 'is_mobile' => false],
    'VN_VariantImages_FirstScreen_RightSide_PC' => ['page' => 'variant', 'id' => '1740633003683', 'size' => [160, 600], 'is_mobile' => false],
    'VN_VariantImages_Sidebar_2_PC' => ['page' => 'variant', 'id' => '1740633029739', 'size' => [300, 250], 'is_mobile' => false],
    'VN_VariantImages_FirstScreen_LeftSide_PC' => ['page' => 'variant', 'id' => '1740632976871', 'size' => [160, 600], 'is_mobile' => false],




    // tools
    'VN_Loantool_Breadcrumb_Above_PC' => ['page' => 'tools', 'id' => '1740633057034', 'size' => [728, 90], 'is_mobile' => false],
    'VN_Loantool_Sidebar_End_PC' => ['page' => 'tools', 'id' => '1740633078701', 'size' => [300, 250], 'is_mobile' => false],
    'VN_Insurancetool_Breadcrumb_Above_PC' => ['page' => 'tools', 'id' => '1740633109519', 'size' => [728, 90], 'is_mobile' => false],
    'VN_Insurancetool_Sidebar_End_PC' => ['page' => 'tools', 'id' => '1740633133048', 'size' => [300, 250], 'is_mobile' => false],
    'MY_Taxtool_Breadcrumb_Above_PC' => ['page' => 'tools', 'id' => '1735652450620', 'size' => [728, 90], 'is_mobile' => false],
    'MY_Taxtool_Sidebar_End_PC' => ['page' => 'tools', 'id' => '1735652611596', 'size' => [300, 250], 'is_mobile' => false],
    'MY_Fuelcosttool_Breadcrumb_Above_PC' => ['page' => 'tools', 'id' => '1735652711805', 'size' => [728, 90], 'is_mobile' => false],
    'MY_Fuelcosttool_Sidebar_End_PC' => ['page' => 'tools', 'id' => '1735652767968', 'size' => [300, 250], 'is_mobile' => false],
    'VN_Fuelpricetool_Breadcrumb_Above_PC' => ['page' => 'tools', 'id' => '1740633212691', 'size' => [728, 90], 'is_mobile' => false],
    'VN_Fuelpricetool_Sidebar_End_PC' => ['page' => 'tools', 'id' => '1740633245602', 'size' => [300, 250], 'is_mobile' => false],
    'VN_Compare_Breadcrumb_Above_PC' => ['page' => 'tools', 'id' => '1740632581185', 'size' => [728, 90], 'is_mobile' => false],
    'MY_Compare_Smyebar_1_PC' => ['page' => 'tools', 'id' => '1735655162916', 'size' => [300, 250], 'is_mobile' => false],
    'VN_Compareresult_Breadcrumb_Above_PC' => ['page' => 'tools', 'id' => '1740632627224', 'size' => [728, 90], 'is_mobile' => false],
    'MY_Compareresult_Smyebar_1_PC' => ['page' => 'tools', 'id' => '1735655347796', 'size' => [300, 250], 'is_mobile' => false],












    // Mobile
    'VN_Hp_Feed_1_Wap' => ['page' => 'home', 'id' => '1740573382277', 'size' => [300, 250], 'is_mobile' => true],
    'MY_Hp_Feed_4_Wap' => ['page' => 'home', 'id' => '1740550577623', 'size' => [336, 280], 'is_mobile' => true],




    'VN_Article_Fourthp_Under_Wap' => ['page' => 'article', 'id' => '1740573447077', 'size' => [300, 250], 'is_mobile' => true],
    'VN_Article_Rating_Under_Wap' => ['page' => 'article', 'id' => '1740573472221', 'size' => [336, 280], 'is_mobile' => true],




    'VN_Model_Variantlist_Above_Wap' => ['page' => 'model', 'id' => '1740573497257', 'size' => [320, 100], 'is_mobile' => true],
    'VN_Model_Variantlist_Under_Wap' => ['page' => 'model', 'id' => '1740573524077', 'size' => [300, 250], 'is_mobile' => true],
    'VN_Model_Gallery_Under_Wap' => ['page' => 'model', 'id' => '1740573567656', 'size' => [336, 280], 'is_mobile' => true],




    'VN_Brand_Usedcar_Above_Wap' => ['page' => 'cars', 'id' => '1740573598877', 'size' => [300, 250], 'is_mobile' => true],




    'TH_Faqs_Relatedmodel_Above_Wap' => ['page' => 'model', 'id' => '1740552067348', 'size' => [300, 250], 'is_mobile' => true],
    'TH_Faqs_Carimages_Under_Wap' => ['page' => 'model', 'id' => '1740552372575', 'size' => [336, 280], 'is_mobile' => true],




    'VN_Modelimages_Relatedmodel_Above_Wap' => ['page' => 'model', 'id' => '1740573668080', 'size' => [300, 250], 'is_mobile' => true],
    'VN_Modelimages_Carimages_Under_Wap' => ['page' => 'model', 'id' => '1740573691977', 'size' => [336, 280], 'is_mobile' => true],




    'VN_Colors_Relatedmodel_Above_Wap' => ['page' => 'model', 'id' => '1740573717941', 'size' => [300, 250], 'is_mobile' => true],
    'VN_Colors_Gallery_Under_Wap' => ['page' => 'model', 'id' => '1740573741895', 'size' => [336, 280], 'is_mobile' => true],




    'VN_Fuelconsumption_Othervariant_Above_Wap' => ['page' => 'model', 'id' => '1740573768696', 'size' => [300, 250], 'is_mobile' => true],
    'VN_Fuelconsumption_Comparison_Above_Wap' => ['page' => 'model', 'id' => '1740573794429', 'size' => [336, 280], 'is_mobile' => true],




    'VN_Userreviews_Relatedmodel_Under_M_Wap' => ['page' => 'model', 'id' => '1740573817369', 'size' => [300, 250], 'is_mobile' => true],




    'VN_Variant_Dealer_Above_Wap' => ['page' => 'variant', 'id' => '1740573840591', 'size' => [300, 250], 'is_mobile' => true],
    'VN_Variant_Carimages_Under_Wap' => ['page' => 'variant', 'id' => '1740573865368', 'size' => [336, 280], 'is_mobile' => true],




    'VN_Variantimages_Related_Above_Wap' => ['page' => 'variant', 'id' => '1740573892425', 'size' => [300, 250], 'is_mobile' => true],
    'VN_Variantimages_Carimages_Under_Wap' => ['page' => 'variant', 'id' => '1740573925132', 'size' => [336, 280], 'is_mobile' => true],




    'VN_Loantool_Moretools_Above_Wap' => ['page' => 'tools', 'id' => '1740573946847', 'size' => [300, 250], 'is_mobile' => true],
    'VN_Loantool_Faq_Above_Wap' => ['page' => 'tools', 'id' => '1740573972475', 'size' => [336, 280], 'is_mobile' => true],




    'VN_Insurancetool_Moretools_Above_Wap' => ['page' => 'tools', 'id' => '1740631656194', 'size' => [300, 250], 'is_mobile' => true],
    'VN_Insurancetool_Faq_Above_Wap' => ['page' => 'tools', 'id' => '1740631680933', 'size' => [336, 280], 'is_mobile' => true],




    'TH_Taxtool_Moretools_Above_Wap' => ['page' => 'tools', 'id' => '1740553113403', 'size' => [300, 250], 'is_mobile' => true],
    'TH_Taxtool_Faq_Above_Wap' => ['page' => 'tools', 'id' => '1740553166075', 'size' => [336, 280], 'is_mobile' => true],




    'TH_Fuelcosttool_Moretools_Above_Wap' => ['page' => 'tools', 'id' => '1740553224039', 'size' => [300, 250], 'is_mobile' => true],
    'TH_Fuelcosttool_Faq_Above_Wap' => ['page' => 'tools', 'id' => '1740553272331', 'size' => [336, 280], 'is_mobile' => true],




    'VN_Fuelpricetool_Moretools_Above_Wap' => ['page' => 'tools', 'id' => '1740631810447', 'size' => [300, 250], 'is_mobile' => true],
    'VN_Fuelpricetool_Faq_Above_Wap' => ['page' => 'tools', 'id' => '1740631838534', 'size' => [336, 280], 'is_mobile' => true],




    'TH_Valuetool_Faq_Above_Wap' => ['page' => 'tools', 'id' => '1736086673824', 'size' => [300, 250], 'is_mobile' => true],




    'TH_Tradeintool_Popularbrand_Above_Wap' => ['page' => 'tools', 'id' => '1740553417292', 'size' => [300, 250], 'is_mobile' => true],
];



function ad_unit_shortcode($atts)
{
    global $ad_units_mapping;

    $atts = shortcode_atts(
        array(
            'ad_id' => 'VN_Hp_Leaderboard_Under_PC',
        ),
        $atts
    );

    $ad_id = $ad_units_mapping[$atts['ad_id']]['id'];
    $min_width = '0px'; //$ad_units_mapping[$atts['ad_id']]['size'][0] . 'px';
    $min_height = '0px'; //$ad_units_mapping[$atts['ad_id']]['size'][1] . 'px';
    $is_mobile = $ad_units_mapping[$atts['ad_id']]['is_mobile'];

    ob_start();

    // Check device type conditionally
    if ($is_mobile && wp_is_mobile() || !$is_mobile && !wp_is_mobile()) : ?>
        <div id='<?php echo 'div-gpt-ad-' . esc_attr($ad_id) . '-0'; ?>'
            style='min-width: <?php echo esc_attr($min_width); ?>; 
                    min-height: <?php echo esc_attr($min_height); ?>;'>
            <script>
                window.addEventListener('load', function() {
                    var adSlot = document.getElementById('div-gpt-ad-<?php echo esc_attr($ad_id); ?>-0');
                    if (adSlot && adSlot.offsetWidth === 0) {
                        adSlot.style.width = '<?php echo esc_attr($ad_units_mapping[$atts['ad_id']]['size'][0] . 'px'); ?>';
                    }
                    googletag.cmd.push(function() {
                        googletag.display('div-gpt-ad-<?php echo esc_attr($ad_id); ?>-0');
                    });
                });
            </script>
        </div>
    <?php endif;

    return ob_get_clean();
}

add_shortcode('dynamic_ad_unit', 'ad_unit_shortcode');

function custom_add_google_fonts()
{
    wp_enqueue_style('custom-google-fonts', 'https://fonts.googleapis.com/css2?family=Roboto+Condensed:wght@400;700&display=swap', false);
}
add_action('wp_enqueue_scripts', 'custom_add_google_fonts');

//function enqueue_swiper_assets()
//{
// wp_enqueue_style('swiper-css', 'https://unpkg.com/swiper/swiper-bundle.min.css');
//  wp_enqueue_script('swiper-js', 'https://unpkg.com/swiper/swiper-bundle.min.js', array('jquery'), null, true);
//}
//add_action('wp_enqueue_scripts', 'enqueue_swiper_assets');

// Global variables
$DOMAIN_NAME = "https://preprod-gcp.wapcar.my/";

// Make it available globally
function set_global_variable()
{
    global $DOMAIN_NAME;
}
// Initialize the global variable
add_action('wp', 'set_global_variable');



// Add custom rewrite rules for language paths
function add_custom_rewrite_rules()
{
    add_rewrite_rule('^zh/?$', 'index.php?language=zh', 'top');
    add_rewrite_rule('^bm/?$', 'index.php?language=bm', 'top');

}
add_action('init', 'add_custom_rewrite_rules');

// Add language query var
function add_language_query_var($vars)
{
    $vars[] = 'language';
    return $vars;
}
add_filter('query_vars', 'add_language_query_var');

// Handle language selection and cookie setting
function handle_language_routing()
{
    // Get current URL path
    $current_path = trim(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH), '/');

    // Cookie settings
    $cookie_name = 'preferred_language';
    $cookie_expiry = time() + (86400 * 30); // 30 days
    $cookie_path = '/';

    // Set language based on URL
    if ($current_path === 'zh') {
        setcookie($cookie_name, '中文', $cookie_expiry, $cookie_path);
    } elseif ($current_path === 'bm') {
        setcookie($cookie_name, 'Bahasa Malaysia', $cookie_expiry, $cookie_path);
    }
    if (strpos($current_path, 'news') !== false) {
        setcookie($cookie_name, 'English', $cookie_expiry, $cookie_path);
    }
}
add_action('template_redirect', 'handle_language_routing');
add_action('init', 'handle_language_routing');

require_once ABSPATH . 'vendor/autoload.php';
//wp_localize_script('your-script-handle', 'ajax_url', admin_url('admin-ajax.php'));

require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/breadcrumb.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/user-menu/user-menu.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/usermenu-sidebar/usermenu-sidebar.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/usermenu-top/usermenu-top.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/usermenu-history/usermenu-history.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/usermenu-add-car/usermenu-add-car.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/login-popup/login-popup.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/login-popup/helpers/validate-token.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/car-popup/car-popup.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/user-cars/user-cars.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/fuel-consumption-info/motor-fuel-consumtion-info.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/fuel-consumption-info/motor-fuel-consumption-view-model.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/fuelConsumption-variant/motor-fuel-consumption-variant.php';

// cars for sale
// if (is_page('used-car-market-value-guide')) {
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/cars-for-sale/car-valuation/car-valuation.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/cars-for-sale/car-valuation/car-valuation-faq/car-valuation-faq.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/cars-for-sale/car-valuation/buying-guides/buying-guides.php';
// require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/cars-for-sale/car-valuation/buying-guides/buying-guides/buying-guides.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/popular-car-brands/brands-in-my.php';
// }

// if (is_page('trade-in-your-car')) {
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/cars-for-sale/trade-in-car/trade-in-car.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/popular-car-brands/brands-in-my.php';
// }
// require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/cars-for-sale/view-all-cars/view-all-cars.php';

require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/recommended-cars-side-shortcode.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/popular-videos.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/navigation-side-widget.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/latest-news-side-shortcode.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/recommended-cars-only-sidewidget.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/videos.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/book-test-drive/book-test-drive.php';

function register_custom_page()
{
    add_shortcode('single_listing_specs_shortcode', 'display_custom_page');
}
add_action('init', 'register_custom_page');

require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/recommended-motor-side-shortcode.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/popular-bike-brands.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/popular-bike-videos.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/recommended-bike-carousel.php';

if (strpos($_SERVER['REQUEST_URI'], '/') !== false || wp_doing_ajax()) {
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/banner-for-home.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/cars-tab-home-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/fuel-prices-home-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/single-thumbnail-news-home.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/videos-home.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/display_cate_news_vertically.php';
    // require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/reviews-home-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/top10-sedans-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/latest-news-home-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/language-switcher.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/brand-logo-lists.php';

    //files for home page motor cycle section
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/motor/brand-logo-lists.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/motor/motor-tabs-home-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/motor/single-thumbnail-news-home.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/motor/latest-news-home-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/motor/videos-home.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/home/motor/display_cate_news_vertically.php';
}

//author page
//rewrite url for author page
// 	function custom_author_rewrite_rules() {
//     add_rewrite_rule(
//         '^author/([^/]+)/?$',
//         'index.php?pagename=author&author_slug=$matches[1]',
//         'top'
//     );
// }
// add_action('init', 'custom_author_rewrite_rules');
// require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/crash-test/test-info-tab.php';
// require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/crash-test/test-rating-shortcode.php';

if (strpos($_SERVER['REQUEST_URI'], 'author') !== false || wp_doing_ajax()) {
    require_once ABSPATH . 'wp-content/themes/voiture-child/authour/authour-latest-news.php';
}

require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/add-car.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/individual-listing-tabs.php';
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/cars-for-sale/used-cars-for-sale/used-cars-for-sale.php';
if (strpos($_SERVER['REQUEST_URI'], 'xe-oto') !== false || wp_doing_ajax()) {
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/gallary.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/mega-gallary.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/competitors.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/near-by-dealers.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/ownership-cost.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/variants.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/news.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/car-overview.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/car-color.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/car-comparision.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/recommended-cars.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/pros-and-cons.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/fuel-consumption.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/fuel-consumption-variant.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/fuel-consumption-info.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/fuel-consumption-view-model.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/related-model.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/find-car-side-widget.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/template-listings/single-listing/social-media-widget.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/footer.php';

    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/individual-news/display_news_content.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/individual-news/tabs.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/individual-car/gallery.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/individual-car/specs.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/individual-car/news.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/individual-listing/faq.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/individual-listing/comparison.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/individual-listing/videos.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/variant-overview.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/individual-listing/ratings/ratings.php';

    //gallery widgets

    require_once ABSPATH . 'wp-content/themes/voiture-child/Gallary/image-cards.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/Gallary/gallary-page.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/Gallary/cars-gallery-carousal.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/Gallary/single-listing-tab-videos-carousal.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/Gallary/car-color-swiper.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/Gallary/top-cars.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/Gallary/gallary-faq.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/Gallary/interior-exterior-side-widget.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/single-listing-specs.php';

    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/news-single-listing.php';

    // individual variant shortcodes
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/variant/overview.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/variant/specification.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/variant/tabs.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/variant/gallery/variant-gallery.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/variant/gallery/variant-exterior-images.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/variant/gallery/variant-interior-images.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/variant/gallery/variant-engine-and-other-images.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/variant/gallery/variant-highlights-images.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/variant/gallery/variant-gallery-carousel.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/variant/single-variant-specs.php';

    //new cars
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/latest-car-videos-carousal-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/brand-sidebar.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/findnew-search-filter.php';
    //     require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/search-filert-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/find-new-cars.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/findnew-car-related-news.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/findnew-car-related-videos.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/findnew-car-comparison.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/findnew-faq-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/findnew-brands.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/findnew-upcoming-cars.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/ev-top-banner-for-newcars.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/new-cars-overview.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/brand-description.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/newcars-variants.php';

    add_shortcode('display_car_brands', 'display_car_brands_alphabetically');

    function load_more_images()
    {
        global $wpdb;

        $offset = isset($_POST['offset']) ? intval($_POST['offset']) : 0;
        $listing_name = sanitize_text_field($_POST['listing_name']);

        // Retrieve the listing post and its variants
        $listing_post = get_posts(array(
            'name' => $listing_name,
            'post_type' => 'listing',
            'posts_per_page' => 1
        ));
        $post_id = $listing_post[0]->ID;

        $parent_variant_ids = $wpdb->get_results($wpdb->prepare(
            "SELECT ID FROM {$wpdb->posts} WHERE post_parent = %d AND post_type = 'variant'",
            $post_id
        ));
        if (!$parent_variant_ids) {
            wp_send_json_error('No variant or parent post found.');
        }

        $ids = array_map(function ($variant) {
            return $variant->ID;
        }, $parent_variant_ids);
        $placeholders = implode(',', array_fill(0, count($ids), '%d'));

        $sql = $wpdb->prepare(
            "SELECT colour, type, image_data FROM car_image WHERE variant_post_id IN ($placeholders)",
            ...$ids
        );
        $data = $wpdb->get_results($sql);

        if (empty($data)) {
            wp_send_json_error('No images found for this variant.');
        }

        // Prepare the next 9 images
        $images = [];
        $exteriorIndex = $offset + 1;
        foreach ($data as $item) {
            if ($item->type === 'Exterior') {
                $image_data = json_decode($item->image_data);
                foreach ($image_data as $image) {
                    if (count($images) >= 9) break;
                    $images[] = [
                        'src' => $image->url,
                        'alt' => $listing_name . ' Exterior ' . str_pad($exteriorIndex++, 3, '0', STR_PAD_LEFT),
                        'title' => $listing_name . ' Exterior ' . str_pad($exteriorIndex - 1, 3, '0', STR_PAD_LEFT),
                        'link' => '/cars/honda/hr-v/car-exterior-image-' . $exteriorIndex
                    ];
                }
            }
        }

        wp_send_json_success(['images' => $images]);
    }

    add_action('wp_ajax_load_more_images', 'load_more_images');
    add_action('wp_ajax_nopriv_load_more_images', 'load_more_images');

    function load_more_interior_images()
    {
        global $wpdb;

        $listing_name = sanitize_text_field($_POST['listing_name']);
        $offset = intval($_POST['offset']);

        // Get the listing post by name
        $listing_post = get_posts(array(
            'name' => $listing_name,
            'post_type' => 'listing',
            'posts_per_page' => 1
        ));

        if (empty($listing_post)) {
            wp_send_json_error('Listing not found.');
        }

        $post_id = $listing_post[0]->ID;

        // Get all variants associated with the listing
        $parent_variant_ids = $wpdb->get_results($wpdb->prepare(
            "SELECT ID FROM {$wpdb->posts} WHERE post_parent = %d AND post_type = 'variant'",
            $post_id
        ));

        if (!$parent_variant_ids) {
            wp_send_json_error('No variant found.');
        }

        $ids = array_map(function ($variant) {
            return $variant->ID;
        }, $parent_variant_ids);

        $placeholders = implode(',', array_fill(0, count($ids), '%d'));

        // Prepare the SQL query with separate placeholders and then add arguments for type and offset
        $sql = $wpdb->prepare(
            "SELECT colour, type, image_data FROM car_image WHERE variant_post_id IN ($placeholders)",
            ...$ids
        );
        $data = $wpdb->get_results($sql);

        if (empty($data)) {
            wp_send_json_error('No more images found.');
        }

        $interior_images_html = '';
        $interiorIndex = $offset + 1;

        foreach ($data as $item) {
            $image_data = json_decode($item->image_data);
            foreach ($image_data as $image) {
                $image_url = esc_url($image->url);
                $alt_text = esc_attr($listing_name . ' Interior ' . str_pad($interiorIndex++, 3, '0', STR_PAD_LEFT));
                $title = esc_attr($listing_name . ' Interior ' . str_pad($interiorIndex - 1, 3, '0', STR_PAD_LEFT));
                $link = esc_url('/cars/honda/hr-v/car-interior-image-' . $interiorIndex);

                $interior_images_html .= '
                <div class="interior-gallery-list-item">
                    <a href="' . $link . '">
                        <img src="' . $image_url . '" title="' . $title . '" alt="' . $alt_text . '">
                    </a>
                    <div>
                        <a href="' . $link . '" class="interior-description-link">' . $title . '</a>
                    </div>
                </div>
            ';
            }
        }

        wp_send_json_success($interior_images_html);
    }
    add_action('wp_ajax_load_more_interior_images', 'load_more_interior_images');
    add_action('wp_ajax_nopriv_load_more_interior_images', 'load_more_interior_images');



    function load_more_other_img_images()
    {
        global $wpdb;

        $listing_name = sanitize_text_field($_POST['listing_name']);
        $offset = intval($_POST['offset']);

        // Get the listing post by name
        $listing_post = get_posts(array(
            'name' => $listing_name,
            'post_type' => 'listing',
            'posts_per_page' => 1
        ));

        if (empty($listing_post)) {
            wp_send_json_error('Listing not found.');
        }

        $post_id = $listing_post[0]->ID;

        // Get all variants associated with the listing
        $parent_variant_ids = $wpdb->get_results($wpdb->prepare(
            "SELECT ID FROM {$wpdb->posts} WHERE post_parent = %d AND post_type = 'variant'",
            $post_id
        ));

        if (!$parent_variant_ids) {
            wp_send_json_error('No variant found.');
        }

        $ids = array_map(function ($variant) {
            return $variant->ID;
        }, $parent_variant_ids);

        $placeholders = implode(',', array_fill(0, count($ids), '%d'));

        // Prepare the SQL query with separate placeholders and then add arguments for type and offset
        $sql = $wpdb->prepare(
            "SELECT colour, type, image_data FROM car_image WHERE variant_post_id IN ($placeholders)",
            ...$ids
        );
        $data = $wpdb->get_results($sql);

        if (empty($data)) {
            wp_send_json_error('No more images found.');
        }

        $other_img_images_html = '';
        $otherImgIndex = $offset + 1;

        foreach ($data as $item) {
            $image_data = json_decode($item->image_data);
            foreach ($image_data as $image) {
                $image_url = esc_url($image->url);
                $alt_text = esc_attr($listing_name . ' Other Image ' . str_pad($otherImgIndex++, 3, '0', STR_PAD_LEFT));
                $title = esc_attr($listing_name . ' Other Image ' . str_pad($otherImgIndex - 1, 3, '0', STR_PAD_LEFT));
                $link = esc_url('/cars/honda/hr-v/car-other-image-' . $otherImgIndex);

                $other_img_images_html .= '
                <div class="other-img-gallery-list-item">
                    <a href="' . $link . '">
                        <img src="' . $image_url . '" title="' . $title . '" alt="' . $alt_text . '">
                    </a>
                    <div>
                        <a href="' . $link . '" class="other-img-description-link">' . $title . '</a>
                    </div>
                </div>
            ';
            }
        }

        wp_send_json_success($other_img_images_html);
    }
    add_action('wp_ajax_load_more_other_img_images', 'load_more_other_img_images');
    add_action('wp_ajax_nopriv_load_more_other_img_images', 'load_more_other_img_images');
}

//motor page
if (strpos($_SERVER['REQUEST_URI'], 'xe-may') !== false || wp_doing_ajax()) {
    //news motorcycles page
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/latest-bike-videos-carousal-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/motor-brand-sidebar.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/findnew-bike-search-filter.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/find-new-bike.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/findnew-motor-related-news.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/findnew-motor-related-videos.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/findnew-motor-comparison.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/motor-findnew-faq-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/motor-findnew-brands.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/findnew-upcoming-motors.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/ev-top-banner-for-newmotor.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/new-motor-overview.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/motor-brand-description.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-motorcycle/newcars-variants.php';

    // individual page ---------------

    // motor individual pages
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/motor-individual-listing-tabs.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-colors.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-comparision.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-overview.php';
    //individual motor page news section
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/news/motor/motor-news-single-listing.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/news/motor/motor-navigation-side-widget.php';
    //individual motor page specs section
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/specs/motor/motor-specs.php';
    //individual motor page gallery section
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/motor-gallery/motor/motor-gallery-carousal.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/motor-gallery/motor/motor-gallary-faq.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/motor-gallery/motor/motor-gallary-page.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/motor-gallery/motor/motor-image-cards.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/motor-gallery/motor/motor-interior-exterior-side-widget.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/motor-gallery/motor/motor-videos-carousal.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/motor-gallery/motor/top-motor.php';
    // require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/competitors.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-gallary.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-mega-gallary.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-near-by-dealers.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-news.php';
    // require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/ownership-cost.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-pros-and-cons.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/recommended-motor.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-variants.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-videos.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-faq.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-fuel-consumption.php';
	require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-template-listings/overview/motor/motor-competitors.php';

    // individual pages end---------------

    // individual motor variant shortcodes
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-variant/overview.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-variant/specification.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-variant/tabs.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-variant/gallery/variant-gallery.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-variant/gallery/variant-exterior-images.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-variant/gallery/variant-interior-images.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-variant/gallery/variant-engine-and-other-images.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-variant/gallery/variant-highlights-images.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-variant/gallery/variant-gallery-carousel.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-variant/single-variant-specs.php';
}

// fuel-price-malaysia
require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/fuel-price-thailand/news-carousal-shortcode.php';
if (strpos($_SERVER['REQUEST_URI'], '/gia-xang-dau') !== false || wp_doing_ajax()) {
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/fuel-price-thailand/latest-fuel-price-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/fuel-price-thailand/petrol-price-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/fuel-price-thailand/fuel-price-faqs.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/fuel-price-thailand/oil-price-desc.php';


    function get_oil_prices()
    {
        $oil_prices = get_posts(array(
            'post_type' => 'oil',
            'posts_per_page' => -1
        ));

        $oil_price_string = '';

        if ($oil_prices) {
            foreach ($oil_prices as $oil_price) {
                $price = get_post_meta($oil_price->ID, 'oil_price', true);
                $title = $oil_price->post_title;

                $oil_price_string .= $title . ' RM ' . $price . ', ';
            }
        }

        return $oil_price_string;
    }
}

// motor calculater pages
if (strpos($_SERVER['REQUEST_URI'], 'dung-cu') !== false || wp_doing_ajax()) {
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator-motor/motor-template-calculators.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator-motor/motor-car-loan-data.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator-motor/motor-buying-guide-news.php';

    //calculator page 
    function fetch_motor_models()
    {
        global $wpdb;
        $brand_id = intval($_POST['brand_id']); // Get selected brand ID

        // Fetch models for the selected brand
        $motor_models = $wpdb->get_results($wpdb->prepare("
          SELECT p.ID, p.post_title
          FROM {$wpdb->posts} p
          INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id
          WHERE pm.meta_key = 'make' 
          AND pm.meta_value = %d
          ORDER BY p.post_title
      ", $brand_id));

        if (!empty($motor_models)) {
            wp_send_json_success($motor_models); // Send models data
        } else {
            wp_send_json_error('No models found');
        }
    }

    function fetch_motor_variants()
    {
        global $wpdb;
        $model_id = intval($_POST['model_id']); // Get selected model ID

        // Fetch variants for the selected model using LIKE to search in serialized data
        $motor_variants = $wpdb->get_results($wpdb->prepare("
          SELECT p.post_title, p.ID
          FROM {$wpdb->posts} p
          INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id
          WHERE pm.meta_key = 'model'
          AND pm.meta_value LIKE %s
      ", '%"' . $model_id . '"%')); // Adjusted for serialized data

        if (!empty($motor_variants)) {
            wp_send_json_success($motor_variants); // Send variants data
        } else {
            wp_send_json_error('No variants found');
        }
    }

    // Register the AJAX actions
    add_action('wp_ajax_fetch_motor_models', 'fetch_motor_models');
    add_action('wp_ajax_fetch_motor_variants', 'fetch_motor_variants');

    add_action('wp_ajax_fetch_motor_variant_data', 'fetch_motor_variant_data');
    add_action('wp_ajax_nopriv_fetch_motor_variant_data', 'fetch_motor_variant_data');

    function fetch_motor_variant_data()
    {
        if (isset($_POST['variant_id'])) {
            $variant_id = intval($_POST['variant_id']);

            // Retrieve the Manufacturers Claim meta value
            $car_price = get_post_meta($variant_id, 'price', true);
            $fuel_consumption = get_post_meta($variant_id, 'manufacturers_claim', true);
            $capacity = get_post_meta($variant_id, 'capacity', true);

            if (empty($fuel_consumption)) {
                $fuel_consumption = 0;
            }
            if (empty($capacity)) {
                $capacity = 0;
            }
            if (empty($car_price)) {
                $car_price = 0;
            }

            wp_send_json_success(['car_price' => $car_price, 'fuel_consumption' => $fuel_consumption, 'capacity' => $capacity]);
        } else {
            wp_send_json_error(['message' => 'Invalid request']);
        }

        wp_die();
    }

    function tools_form_motor_calculator_script()
    {
        wp_enqueue_script('tools-form-calculator-script', get_stylesheet_directory_uri() . 'widget-shortcodes/calculator-motor/js/motor-calculator.js', array('jquery'), null, true);

        wp_localize_script('tools-form-calculator-script', 'ajax_data', array(
            'ajax_url' => admin_url('admin-ajax.php')
        ));
    }
    add_action('wp_enqueue_scripts', 'tools_form_motor_calculator_script');
}

//compare cars
// if (is_page('compare-cars') && preg_match('#^/compare-cars/?#', $_SERVER['REQUEST_URI']) || wp_doing_ajax()) {
if (strpos($_SERVER['REQUEST_URI'], 'so-sanh-xe') !== false || wp_doing_ajax()) {
    // wp-content\themes\voiture-child\widget-shortcodes\car-comparison\car-compare-shortcode.php
    require_once(get_stylesheet_directory() . '/widget-shortcodes/car-comparison/car-compare-shortcode.php');
    require_once(get_stylesheet_directory() . '/widget-shortcodes/car-comparison/compare-popular-cars-tabs.php');
    require_once(get_stylesheet_directory() . '/widget-shortcodes/car-comparison/car-videos-comparisonpage.php');
    require_once(get_stylesheet_directory() . '/widget-shortcodes/car-comparison/compare-faq.php');
    require_once(get_stylesheet_directory() . '/widget-shortcodes/car-comparison/comparison-news-carousel.php');
}

//motor comparison pages:
if (strpos($_SERVER['REQUEST_URI'], 'so-sanh-xe-may') !== false || wp_doing_ajax()) {
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-comparison/compare-popular-motor-tabs.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-comparison/motor_compare-faq.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-comparison/motor-compare-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/motor-comparison/motor-videos-comparisonpage.php';
}
if (
    strpos($_SERVER['REQUEST_URI'], 'xe-oto') !== false ||
    strpos($_SERVER['REQUEST_URI'], 'tin-tuc') !== false ||
    strpos($_SERVER['REQUEST_URI'], '/zh') === 0 ||
    strpos($_SERVER['REQUEST_URI'], '/bm') === 0 ||
    wp_doing_ajax()
) {
    require_once ABSPATH . 'wp-content/themes/voiture-child/article-page.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/social-media.php';
}
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/recommended-car-carousel.php';

if (strpos($_SERVER['REQUEST_URI'], 'xe-oto') !== false || strpos($_SERVER['REQUEST_URI'], 'dung-cu') !== false || strpos($_SERVER['REQUEST_URI'], 'gia-xang-dau') !== false || strpos($_SERVER['REQUEST_URI'], 'tin-tuc') !== false || wp_doing_ajax()) {
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/popular-car-brands/brands-in-my.php';
}

function custom_calculator_rewrite()
{
    // Specific rewrite rules for trade-in-your-car and used-car-market-value-guide
    add_rewrite_rule(
        '^tools/trade-in-your-car/?$', // Match /tools/trade-in-your-car
        'index.php?pagename=trade-in-your-car', // Map to the trade-in-your-car page
        'top'
    );

    add_rewrite_rule(
        '^tools/used-car-market-value-guide/?$', // Match /tools/used-car-market-value-guide
        'index.php?pagename=used-car-market-value-guide', // Map to the used-car-market-value-guide page
        'top'
    );

//     // Generic rewrite rule for other /tools/ paths
//     add_rewrite_rule(
//         '^tools/([^/]*)/?', // Match /tools/<anything>
//         'index.php?pagename=tools', // Map to the tools page
//         'top'
//     );
	    // Match /alat/kredit-motor
    // Match /alat/kredit-motor
    add_rewrite_rule('^alat/kredit-motor/?$', 'index.php?pagename=kredit-motor', 'top');

    // Match /alat/anything-else or /alat
    add_rewrite_rule('^dung-cu/([^/]*)/?$', 'index.php?pagename=tools', 'top');
	
}
add_action('init', 'custom_calculator_rewrite');


// if (is_page('tools') && preg_match('#^/tools/?#', $_SERVER['REQUEST_URI']) || wp_doing_ajax()) {
if (strpos($_SERVER['REQUEST_URI'], '/dung-cu/mua-xe-tra-gop') !== false ||strpos($_SERVER['REQUEST_URI'], '/dung-cu/bao-hiem-xe') !== false || wp_doing_ajax()) {
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator/template-calculators.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator/car-loan-data.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator/fuel-cost-data.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator/insurance-data.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator/road-tax-data.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator/more-tools-shortcode.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator/buying-guide.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator/related-news.php';

    //calculator page 
    function fetch_car_models()
    {
        global $wpdb;
        $brand_id = intval($_POST['brand_id']); // Get selected brand ID

        // Fetch models for the selected brand
        $car_models = $wpdb->get_results($wpdb->prepare("
						SELECT DISTINCT p.ID, p.post_title
						FROM {$wpdb->posts} p
						INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id
						WHERE pm.meta_key = '_listing_make' 
						AND pm.meta_value = %d
						ORDER BY p.post_title
					", $brand_id));

        if (!empty($car_models)) {
            wp_send_json_success($car_models); // Send models data
        } else {
            wp_send_json_error('No models found');
        }
    }

    function fetch_car_variants()
    {
        global $wpdb;
        $model_id = intval($_POST['model_id']); // Get selected model ID

        // Fetch variants for the selected model using LIKE to search in serialized data
        $car_variants = $wpdb->get_results($wpdb->prepare("
            SELECT p.post_title, p.ID
            FROM {$wpdb->posts} p
            INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id
            WHERE pm.meta_key = 'model'
            AND pm.meta_value LIKE %s
        ", '%"' . $model_id . '"%')); // Adjusted for serialized data

        if (!empty($car_variants)) {
            wp_send_json_success($car_variants); // Send variants data
        } else {
            wp_send_json_error('No variants found');
        }
    }

    // Register the AJAX actions
    add_action('wp_ajax_fetch_car_models', 'fetch_car_models');
    add_action('wp_ajax_nopriv_fetch_car_models', 'fetch_car_models'); // For non-logged-in users
    add_action('wp_ajax_fetch_car_variants', 'fetch_car_variants');
    add_action('wp_ajax_nopriv_fetch_car_variants', 'fetch_car_variants'); // For non-logged-in users


    function fetch_car_variant_data()
    {
        if (isset($_POST['variant_id'])) {
            $variant_id = intval($_POST['variant_id']);

            // Retrieve the Manufacturers Claim meta value
            $car_price = get_post_meta($variant_id, 'retail_price', true);
            $fuel_consumption = get_post_meta($variant_id, 'manufacturers_claim', true);
            $capacity = get_post_meta($variant_id, 'capacity', true);

            if (empty($fuel_consumption)) {
                $fuel_consumption = 0;
            }
            if (empty($capacity)) {
                $capacity = 0;
            }
            if (empty($car_price)) {
                $car_price = 0;
            }

            wp_send_json_success(['car_price' => $car_price, 'fuel_consumption' => $fuel_consumption, 'capacity' => $capacity]);
        } else {
            wp_send_json_error(['message' => 'Invalid request']);
        }

        wp_die();
    }

    add_action('wp_ajax_fetch_car_variant_data', 'fetch_car_variant_data');
    add_action('wp_ajax_nopriv_fetch_car_variant_data', 'fetch_car_variant_data');

    //rewrite url for template calculator
    function custom_rewrite_flush()
    {
        custom_calculator_rewrite();
        flush_rewrite_rules();
    }
    add_action('after_switch_theme', 'custom_rewrite_flush');

    function tools_form_calculator_script()
    {
        wp_enqueue_script('tools-form-calculator-script', get_stylesheet_directory_uri() . '/js/tools-calculator.js', array('jquery'), null, true);

        wp_localize_script('tools-form-calculator-script', 'ajax_data', array(
            'ajax_url' => admin_url('admin-ajax.php')
        ));
    }
    add_action('wp_enqueue_scripts', 'tools_form_calculator_script');
}

// if (is_page('cars-electric') && preg_match('#^/cars-electric/?#', $_SERVER['REQUEST_URI']) || wp_doing_ajax()) {
if (strpos($_SERVER['REQUEST_URI'], 'mobil-listrik') !== false || wp_doing_ajax()) {
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/ev/brands-list.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/ev/display_cate_news_vertically.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/ev/ev-car-comparison.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/ev/ev-faq.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/ev/ev-range-ranking.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/ev/ev-technology-news.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/ev/ev-upcoming.php';
    require_once ABSPATH .  'wp-content/themes/voiture-child/widget-shortcodes/ev/ev-videos.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/ev/latest-ev-news.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/ev/popular_ev_cars.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/ev/ev-car-tools/ev-car-tools.php';
}


function enqueue_toggle_scripts()
{
    wp_enqueue_script('toggle-script', get_stylesheet_directory_uri() . '/js/toggle-btn.js', array('jquery'), null, true);

    wp_localize_script('toggle-script', 'ajax_data', array(
        'ajax_url' => admin_url('admin-ajax.php')
    ));
}
add_action('wp_enqueue_scripts', 'enqueue_toggle_scripts');
//News page
$news_categories = get_terms(array(
    'taxonomy' => 'news-category',
    'hide_empty' => true
));

$is_news_related = false;

// Check if the URL contains 'news/latest' or any news category
if (strpos($_SERVER['REQUEST_URI'], 'news/latest') !== false) {
    $is_news_related = true;
} else {
    foreach ($news_categories as $category) {
        if (is_object($category) && isset($category->slug)) {
            if (strpos($_SERVER['REQUEST_URI'], '/news/' . $category->slug) !== false) {
                // Ensure it is not part of another path like '/cars/honda/city/news/'
                $path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
                $segments = explode('/', trim($path, '/'));

                // Check if 'news' is followed by a category slug, ensuring it's a valid category page
                if (end($segments) === $category->slug) {
                    $is_news_related = true;
                    break;
                }
            }
        }
    }
}

/***************************redirection rules *************************************/
function custom_amp_rewrite_rule()
{
    add_rewrite_rule('^news/([0-9]+)/amp/?$', 'index.php?custom_amp_news_id=$matches[1]', 'top');
}
add_action('init', 'custom_amp_rewrite_rule');

function custom_motor_amp_rewrite_rule()
{
    add_rewrite_rule('^news-motorcycles/([0-9]+)/amp/?$', 'index.php?custom_amp_news_id=$matches[1]', 'top');
}
add_action('init', 'custom_motor_amp_rewrite_rule');

function custom_amp_query_vars($query_vars)
{
    $query_vars[] = 'custom_amp_news_id';
    return $query_vars;
}
add_filter('query_vars', 'custom_amp_query_vars');

function custom_amp_template_redirect()
{
    $news_id = get_query_var('custom_amp_news_id');
    if ($news_id) {
        global $wpdb;
        $result = $wpdb->get_var(
            $wpdb->prepare("SELECT news_post_id FROM news_temp WHERE news_id = %d", $news_id)
        );
        $post = get_post($result);
        if ($post) {
            $post_name = $post->post_name;
            $redirection_url = home_url('/news/' . $post_name . '-' . $news_id);
            header("Location: $redirection_url", true, 302);
            exit;
        } else {
            $redirection_url = home_url('/news/');
            header("Location: $redirection_url", true, 302);
        }

        exit; // Ensure no further execution
    }
}
add_action('template_redirect', 'custom_amp_template_redirect');

function wapcar_custom_rewrite_rules()
{
    add_rewrite_rule(
        '^car-loan(/.*)?$',
        'index.php?loan_calculator=1',
        'top'
    );

    add_rewrite_rule(
        '^car-insurance(/.*)?$',
        'index.php?insurance_calculator=1',
        'top'
    );

    add_rewrite_rule(
        '^cars/([^/]+)/amp/?$',
        'index.php?car_make=$matches[1]',
        'top'
    );

    add_rewrite_rule(
        '^dealers(/.*)?$',
        'index.php?dealers=1',
        'top'
    );

    add_rewrite_rule(
        '^news-images/([^/]+)$',
        'index.php?news_image=$matches[1]',
        'top'
    );

    add_rewrite_rule(
        '^(new-faqs|kumpul-kereta|collect-faqs|petrol-station|engine-oil|topicsinfor)(/.*)?$',
        'index.php',
        'top'
    );
}
add_action('init', 'wapcar_custom_rewrite_rules');

function wapcar_add_query_vars_for_miscl($vars)
{
    $vars[] = 'loan_calculator';
    $vars[] = 'insurance_calculator';
    $vars[] = 'car_make';
    $vars[] = 'dealers';
    $vars[] = 'news_image';
    return $vars;
}
add_filter('query_vars', 'wapcar_add_query_vars_for_miscl');

// Handle redirections 
function wapcar_handle_redirections()
{
    if (is_admin() || wp_doing_ajax()) {
        return;
    }

    $current_url = $_SERVER['REQUEST_URI'];

    // Car Loan Redirection
    if (get_query_var('loan_calculator')) {
        wp_redirect(home_url('/tools/loan-calculator'), 301);
        exit;
    }

    // Car Insurance Redirection
    if (get_query_var('insurance_calculator')) {
        wp_redirect(home_url('/tools/insurance-calculator'), 301);
        exit;
    }

    // AMP Redirection
    if (preg_match('#^/cars/([^/]+)/amp$#', $current_url, $matches)) {
        wp_redirect(home_url('/cars/' . $matches[1]), 301);
        exit;
    }

    // Dealers Redirection
    if (get_query_var('dealers')) {
        wp_redirect(home_url('/cars-for-sale/malaysia'), 301);
        exit;
    }

    // News Images Redirection
    if (get_query_var('news_image')) {
        $news_slug = preg_replace('/-[a-f0-9]{32}$/', '', get_query_var('news_image'));
        wp_redirect(home_url('/news/' . $news_slug), 301);
        exit;
    }
}
add_action('template_redirect', 'wapcar_handle_redirections');

/********************************* redirection rules end ************************************/

// Example usage of the unified condition
$current_url = $_SERVER['REQUEST_URI'];
if (
    preg_match('/^\/news(\/|$)/', $current_url) ||
    wp_doing_ajax() ||
    strpos($_SERVER['REQUEST_URI'], '/zh') === 0 ||
    strpos($_SERVER['REQUEST_URI'], '/bm') === 0
) {
    function enqueue_custom_scripts()
    {
        wp_enqueue_script('custom-news-script', get_stylesheet_directory_uri() . '/js/custom-news.js', array('jquery'), null, true);

        wp_localize_script('custom-news-script', 'ajax_data', array(
            'ajax_url' => admin_url('admin-ajax.php')
        ));
    }
    add_action('wp_enqueue_scripts', 'enqueue_custom_scripts');

    function load_news_template_based_on_url($template)
    {
        // Get the current URL
        global $wp;

        $current_url = home_url(add_query_arg(array(), $wp->request));
        // Ensure $current_url is a string (handle null case)
        $current_url = $current_url ?? '';

        // Check if the current URL contains 'news'
        if (
            strpos($current_url, 'news') !== false ||
            strpos($current_url, '/zh') !== false ||
            strpos($current_url, '/bm') !== false
        ) {
            add_filter('pre_get_document_title', function ($title) {
                return 'News - ' . get_bloginfo('name');
            });
            // Load the archive-news.php template
            //             return get_stylesheet_directory() . '/archive-news.php';
        }

        // Return the default template if 'news' is not in the URL
        return $template;
    }
    add_filter('template_include', 'load_news_template_based_on_url');

    //load news data
    function load_subcategory_news()
    {
        error_log('trigger news files');
        // Sanitize and retrieve POST data
        $category_id = isset($_POST['category_id']) ? intval($_POST['category_id']) : 0;
        $subcategory_id = isset($_POST['subcategory_id']) ? intval($_POST['subcategory_id']) : $category_id;
        $paged = isset($_POST['paged']) ? intval($_POST['paged']) : 1;
        $posts_per_page = 10;
        $subcategories = '';
        $category = get_term($category_id, 'news-category');
        // second language
        //         $second_lang = get_current_language();

        $cache_key = 'subcategory_news_' . $category_id . '_' . $subcategory_id . '_' . $paged;
        $cached_data = get_data_from_redis($cache_key);
        if ($cached_data) {
            echo $cached_data;
            wp_die();
        }

        // Get subcategories for the selected category
        if ($category_id != 0) {
            global $wpdb;

            $query = $wpdb->prepare("
            SELECT DISTINCT t.*
            FROM {$wpdb->terms} AS t
            INNER JOIN {$wpdb->term_taxonomy} AS tt ON t.term_id = tt.term_id
            LEFT JOIN {$wpdb->termmeta} AS tm1 ON t.term_id = tm1.term_id AND tm1.meta_key = %s
            LEFT JOIN {$wpdb->termmeta} AS tm2 ON t.term_id = tm2.term_id AND tm2.meta_key = %s
            LEFT JOIN {$wpdb->termmeta} AS tm3 ON t.term_id = tm3.term_id AND tm3.meta_key = %s
            WHERE tt.parent = %d
              AND tt.taxonomy = %s
              AND (tm1.meta_value = '' OR tm1.meta_value IS NULL)  -- second_lang is empty
              AND tm2.meta_value = %s                              -- state = '1'
              AND tm3.meta_value = %s                              -- type = '1'
            ORDER BY CAST(tm1.meta_value AS UNSIGNED) ASC          -- sort by 'sort' meta value numerically
        ", 'second_lang', 'state', 'type', $category_id, 'news-category', '1', '1');

            $subcategories = $wpdb->get_results($query);
        }

        $subcategory_html = '';

        if (!empty($subcategories) && !is_wp_error($subcategories)) {
            $subcategory_html .= '<div class="subcategory-row">';
            $subcategory_html .= '<div class="subcategory-tab sub-active first-sub" data-category="' . esc_attr($category_id) . '"data-subcategory="' . esc_attr($category_id) . '"><a>All</a></div>';
            foreach ($subcategories as $subcategory) {
                $subcategory_html .= '<div class="subcategory-tab" 
                data-subcategory="' . esc_attr($subcategory->term_id) . '"
                data-subcategoryname="' . esc_attr($subcategory->name) . '"
                data-categoryname="' . esc_attr($category->name) . '"><a>'
                    . esc_html($subcategory->name) . '</a></div>';
            }
            $subcategory_html .= '</div>';
        } else {
            $terms = get_terms(array(
                'taxonomy'   => 'news-category',
                'name'       => 'Others',
                'hide_empty' => false,
                'parent' => $category_id,
            ));

            $subcategory_id = $terms[0]->term_id;
        }

        $query_args = array(
            'post_type'      => 'news',
            'posts_per_page' => $posts_per_page,
            'paged'          => $paged,
            'post_status'    => 'publish',

            'meta_query'     => array(
                array(
                    'key'     => 'second_language',
                    'value'   => '',
                    'compare' => '='
                ),
                array(
                    'key'     => 'publish_time',
                    'value'   => current_time('mysql'),
                    'compare' => '<',
                    'type'    => 'DATETIME'
                )
            ),
        );

        if ($category_id == 0) {
        } elseif ($subcategory_id == $category_id && !empty($subcategories) && !is_wp_error($subcategories)) {
            // Multiple subcategories condition
            $meta_queries = array_map(function ($subcategory) {
                return array(
                    'key'     => 'news_category',
                    'value'   => sprintf(':"%d";', $subcategory->term_id),
                    'compare' => 'LIKE',
                );
            }, $subcategories);

            $query_args['meta_query'][] = array_merge(['relation' => 'OR'], $meta_queries);
        } else {
            // Single subcategory condition
            $query_args['meta_query'][] = array(
                'key'     => 'news_category',
                'value'   => sprintf(':"%d";', $subcategory_id),
                'compare' => 'LIKE',
            );
        }

        // Execute the query
        $news_query = new WP_Query($query_args);

        ob_start();

        if ($news_query->have_posts()) {
            while ($news_query->have_posts()) {
                $news_query->the_post();
                get_template_part('template-posts/loop/inner-list', get_post_format());
            }
        } else {
            echo '<p></p>';
        }

        $news_html = ob_get_clean();
        wp_reset_postdata();

        // Check if there are more posts to load
        $has_more = ($paged * $posts_per_page) < $news_query->found_posts;

        echo json_encode(array(
            'subcategories' => $subcategory_html,
            'posts' => $news_html,
            'has_more' => $has_more, // Flag to indicate more posts
        ));

        set_data_to_redis($cache_key, json_encode(array(
            'subcategories' => $subcategory_html,
            'posts' => $news_html,
            'has_more' => $has_more
        )));

        wp_die();
    }

    add_action('wp_ajax_load_subcategory_news', 'load_subcategory_news');
    add_action('wp_ajax_nopriv_load_subcategory_news', 'load_subcategory_news');
}

//motorcycle news page
$current_url = $_SERVER['REQUEST_URI'];
if (preg_match('/tin-tuc-xe-may/', $current_url) || wp_doing_ajax()) {
    function enqueue_custom_motor_scripts()
    {
        wp_enqueue_script('custom-motor-news-script', get_stylesheet_directory_uri() . '/js/custom-motor-news.js', array('jquery'), null, true);

        wp_localize_script('custom-motor-news-script', 'ajax_data', array(
            'ajax_url' => admin_url('admin-ajax.php')
        ));
    }
    add_action('wp_enqueue_scripts', 'enqueue_custom_motor_scripts');

    function load_motor_news_template_based_on_url($template)
    {
        // Get the current URL
        $current_url = home_url(add_query_arg(array(), $wp->request));

        // Check if the current URL contains 'news'
        if (strpos($current_url, 'news-motorcycles') !== false) {
            add_filter('pre_get_document_title', function ($title) {
                return 'News - ' . get_bloginfo('name');
            });
            // Load the archive-news-motorcycles.php template
            return get_stylesheet_directory() . '/archive-news-motorcycles.php';
        }

        // Return the default template if 'news' is not in the URL
        return $template;
    }
    add_filter('template_include', 'load_motor_news_template_based_on_url');

    //load news data
    function load_subcategory_motor_news()
    {
        // Sanitize and retrieve POST data
        $category_id = isset($_POST['category_id']) ? intval($_POST['category_id']) : 0;
        $subcategory_id = isset($_POST['subcategory_id']) ? intval($_POST['subcategory_id']) : $category_id;
        $paged = isset($_POST['paged']) ? intval($_POST['paged']) : 1;
        $posts_per_page = 10;
        $subcategories = '';
        $category = get_term($category_id, 'motorcycle-news-category');
		
        // second language
        //         $second_lang = get_current_language();

        $cache_key = 'motor_subcategory_news_' . $category_id . '_' . $subcategory_id . '_' . $paged;
        $cached_data = get_data_from_redis($cache_key);
        if ($cached_data) {
            echo $cached_data;
            wp_die();
        }

        // Get subcategories for the selected category
        if ($category_id != 0) {
            global $wpdb;

            $query = $wpdb->prepare("
            SELECT DISTINCT t.*
            FROM {$wpdb->terms} AS t
            INNER JOIN {$wpdb->term_taxonomy} AS tt ON t.term_id = tt.term_id
            LEFT JOIN {$wpdb->termmeta} AS tm1 ON t.term_id = tm1.term_id AND tm1.meta_key = %s
            LEFT JOIN {$wpdb->termmeta} AS tm2 ON t.term_id = tm2.term_id AND tm2.meta_key = %s
            LEFT JOIN {$wpdb->termmeta} AS tm3 ON t.term_id = tm3.term_id AND tm3.meta_key = %s
            WHERE tt.parent = %d
              AND tt.taxonomy = %s
              AND (tm1.meta_value = '' OR tm1.meta_value IS NULL)  -- second_lang is empty
              AND tm2.meta_value = %s                              -- state = '1'
              AND tm3.meta_value = %s                              -- type = '1'
            ORDER BY CAST(tm1.meta_value AS UNSIGNED) ASC          -- sort by 'sort' meta value numerically
        ", 'second_lang', 'state', 'type', $category_id, 'motorcycle-news-category', '1', '1');

            $subcategories = $wpdb->get_results($query);
        }

        $subcategory_html = '';

        if (!empty($subcategories) && !is_wp_error($subcategories)) {
            $subcategory_html .= '<div class="subcategory-row">';
            $subcategory_html .= '<div class="subcategory-tab sub-active first-sub" data-category="' . esc_attr($category_id) . '"data-subcategory="' . esc_attr($category_id) . '"><a>All</a></div>';
            foreach ($subcategories as $subcategory) {
                $subcategory_html .= '<div class="subcategory-tab" 
                data-subcategory="' . esc_attr($subcategory->term_id) . '"
                data-subcategoryname="' . esc_attr($subcategory->name) . '"
                data-categoryname="' . esc_attr($category->name) . '"><a>'
                    . esc_html($subcategory->name) . '</a></div>';
            }
            $subcategory_html .= '</div>';
        } elseif ($category_id != 0 || $category_id === $subcategory_id) {
            $terms = get_terms(array(
                'taxonomy'   => 'motorcycle-news-category',
                'name'       => 'Others',
                'hide_empty' => false,
                'parent' => $category_id,
            ));

            $subcategory_id = $terms[0] ? $terms[0]->term_id : 0;
        }

        $query_args = array(
            'post_type'      => 'motorcycle-news',
            'posts_per_page' => $posts_per_page,
            'paged'          => $paged,
            'post_status'    => 'publish',

            'meta_query'     => array(
                //                 array(
                //                     'key'     => 'second_language',
                //                     'value'   => '',
                //                     'compare' => '='
                //                 ),
                array(
                    'key'     => 'publish_time',
                    'value'   => current_time('mysql'),
                    'compare' => '<',
                    'type'    => 'DATETIME'
                )
            ),
        );

        if (!empty($subcategories) && !is_wp_error($subcategories)) {
            // Multiple subcategories condition
            $meta_queries = array_map(function ($subcategory) {
                return array(
                    'key'     => 'motorcycle-news-category',
                    'value'   => sprintf(':"%d";', $subcategory->term_id),
                    'compare' => 'LIKE',
                );
            }, $subcategories);

            $query_args['meta_query'][] = array_merge(['relation' => 'OR'], $meta_queries);
        } elseif(!empty($subcategory_id)) {
            // Single subcategory condition
            $query_args['meta_query'][] = array(
                'key'     => 'motorcycle-news-category',
                'value'   => sprintf(':"%d";', $subcategory_id),
                'compare' => 'LIKE',
            );
        }

        // Execute the query
        $news_query = new WP_Query($query_args);

        ob_start();

        if ($news_query->have_posts()) {
            while ($news_query->have_posts()) {
                $news_query->the_post();
                get_template_part('template-posts/loop/inner-list', get_post_format());
            }
        } else {
            echo '<p>No News Available for this category</p>';
        }

        $news_html = ob_get_clean();
        wp_reset_postdata();

        // Check if there are more posts to load
        $has_more = ($paged * $posts_per_page) < $news_query->found_posts;

        echo json_encode(array(
            'subcategories' => $subcategory_html,
            'posts' => $news_html,
            'has_more' => $has_more, // Flag to indicate more posts
        ));

        set_data_to_redis($cache_key, json_encode(array(
            'subcategories' => $subcategory_html,
            'posts' => $news_html,
            'has_more' => $has_more
        )));

        wp_die();
    }

    add_action('wp_ajax_load_subcategory_motor_news', 'load_subcategory_motor_news');
    add_action('wp_ajax_nopriv_load_subcategory_motor_news', 'load_subcategory_motor_news');
}


function wapcar_add_query_vars($vars)
{
    $vars[] = 'make';
    $vars[] = 'model';
    $vars[] = 'sub_page'; // Add sub_page query var
    return $vars;
}
add_filter('query_vars', 'wapcar_add_query_vars');

function capture_dynamic_variables()
{
    $make = get_query_var('make');
    $model = get_query_var('model');
}
add_action('template_redirect', 'capture_dynamic_variables');

function create_cars_post_type()
{
    register_post_type(
        'xe-oto',
        array(
            'labels' => array(
                'name' => __('Xe-oto'),
                'singular_name' => __('Xe-oto'),
            ),
            'public' => true,
            'has_archive' => true,
            'rewrite' => array('slug' => 'xe-oto/%make%/%model%'), // Include the make and model in the rewrite
            'supports' => array('title', 'editor', 'custom-fields'),
        )
    );
}
add_action('init', 'create_cars_post_type');

function create_motors_post_type()
{
    register_post_type(
        'Motorcycles',
        array(
            'labels' => array(
                'name' => __('Xe-may'),
                'singular_name' => __('Xe-may'),
            ),
            'public' => true,
            'has_archive' => true,
            'rewrite' => array('slug' => 'xe-may/%make%/%model%'), // Include the make and model in the rewrite
            'supports' => array('title', 'editor', 'custom-fields'),
        )
    );
}
add_action('init', 'create_motors_post_type');
// Define the list of slugs to exclude from redirection
function is_excluded_news_slug($slug, $is_motorcycle = false)
{
    if ($is_motorcycle) {
        $news_categories = get_terms(array(
            'taxonomy' => 'motorcycle-news-category',
            'hide_empty' => false,
            'parent' => 0
        ));
    } else {
        $news_categories = get_terms(array(
            'taxonomy' => 'news-category',
            'hide_empty' => false,
            'parent' => 0
        ));
    }
    $excluded_slugs = array_map(function ($category) {
        return $category->slug;
    }, $news_categories);

    if ($is_motorcycle) {
        $excluded_slugs = array('latest', 'review', 'buying-guide', 'tips');
    } else {
		$excluded_slugs = array('latest', 'reviews', 'buying-guides', 'evs', 'tips', 'used-car', 'comments');
    }

    return in_array($slug, $excluded_slugs);
}


add_action('init', 'custom_cars_rewrite_rules');

function custom_cars_rewrite_rules()
{
    add_rewrite_rule('^tin-tuc/([^/]+)/?$', 'index.php?post_type=news&news_slug=$matches[1]', 'top');
    add_rewrite_rule('^xe-oto/([^/]+)/?$', 'index.php?pagename=New Cars&make=$matches[1]', 'top');
    add_rewrite_rule('^xe-oto/([^/]+)/([^/]+)/?$', 'index.php?post_type=xe-oto&make=$matches[1]&model=$matches[2]', 'top');
    add_rewrite_rule('^xe-oto/([^/]+)/([^/]+)/([^/]+)/?$', 'index.php?post_type=xe-oto&make=$matches[1]&model=$matches[2]&section=$matches[3]', 'top');
    add_rewrite_rule('^xe-oto/([^/]+)/([^/]+)/([^/]+)/([^/]+)/?$', 'index.php?post_type=xe-oto&make=$matches[1]&model=$matches[2]&section=$matches[3]&variant_section=$matches[4]', 'top');

    add_rewrite_rule('^mobil-baru/([^/]+)/?$', 'index.php?pagename=xe-oto&filter=$matches[1]', 'top');

    //motor rewrite url
    add_rewrite_rule('^tin-tuc-xe-may/([^/]+)/?$', 'index.php?post_type=tin-tuc-xe-may&news_slug=$matches[1]', 'top');
    add_rewrite_rule('^xe-may/([^/]+)/?$', 'index.php?pagename=new motorcycles&make=$matches[1]', 'top');
    add_rewrite_rule('^xe-may/([^/]+)/([^/]+)/?$', 'index.php?post_type=xe-may&make=$matches[1]&model=$matches[2]', 'top');
    add_rewrite_rule('^xe-may/([^/]+)/([^/]+)/([^/]+)/?$', 'index.php?post_type=xe-may&make=$matches[1]&model=$matches[2]&section=$matches[3]', 'top');
    add_rewrite_rule('^xe-may/([^/]+)/([^/]+)/([^/]+)/([^/]+)/?$', 'index.php?post_type=xe-may&make=$matches[1]&model=$matches[2]&section=$matches[3]&variant_section=$matches[4]', 'top');

    add_rewrite_rule('^xe-may/([^/]+)/?$', 'index.php?pagename=new motorcycles&filter=$matches[1]', 'top');

    //author
    add_rewrite_rule('^author/([^/]+)/?$', 'index.php?pagename=author&author_slug=$matches[1]', 'top');
}
// Register the custom query variable
function add_custom_news_query_var($vars)
{
    $vars[] = 'news_slug';
    return $vars;
}
add_filter('query_vars', 'add_custom_news_query_var');

function add_custom_car_query_vars($vars)
{
    $vars[] = 'make';
    return $vars;
}
add_filter('query_vars', 'add_custom_car_query_vars');

// rewrite rules fot zh, bm article pages
function add_language_news_rewrite_rules()
{
    // Add rewrite rule for both zh and bm URLs
    add_rewrite_rule(
        '^(zh|bm)/([^/]+)-(\d+)/?$',
        'index.php?lang=$matches[1]&post_name=$matches[2]&post_id=$matches[3]',
        'top'
    );
}
add_action('init', 'add_language_news_rewrite_rules');

function add_language_news_query_vars($vars)
{
    $vars[] = 'lang';
    $vars[] = 'post_name';
    $vars[] = 'post_id';
    return $vars;
}
add_filter('query_vars', 'add_language_news_query_vars');

// Load archive-cars.php template for zh, bm language URLs
function language_news_template_redirect()
{
    // Get the query variables
    $lang = get_query_var('lang');
    $post_name = get_query_var('post_name');
    $post_id = get_query_var('post_id');

    // Check if we have all the required parameters
    if ($lang && $post_name && $post_id) {
        // Verify if the language is either 'zh' or 'bm'
        if (in_array($lang, ['zh', 'bm'])) {
            // Load the archive-cars.php template
            $template = get_stylesheet_directory() . '/archive-xe-oto.php';
            if (file_exists($template)) {
                load_template($template);
                exit;
            }
        }
    }
}
add_action('template_redirect', 'language_news_template_redirect');



// Redirect non-excluded news/slug URLs to archive-cars.php
add_filter('template_include', 'load_archive_cars_for_news_slug');
function load_archive_cars_for_news_slug($template)
{
    // 	print_r('<h1>Hello world!</h1>');
    // 	exit;
    $news_slug = get_query_var('news_slug');

    if (($news_slug && !is_excluded_news_slug($news_slug))) {
        return get_stylesheet_directory() . '/archive-xe-oto.php';
    }

    // 	print_r('above news slug condition');
    if (is_excluded_news_slug($news_slug) && $news_slug) {
        return get_stylesheet_directory() . '/archive-news.php';
    }
    // 	print_r('returning default template');

    return $template;
}

// Redirect non-excluded news/slug URLs to archive-motors.php
add_filter('template_include', 'load_archive_motors_for_news_slug');
function load_archive_motors_for_news_slug($template)
{
    // 	print_r($_SERVER['REQUEST_URI']);
    if (strpos($_SERVER['REQUEST_URI'], 'xe-may') !== false || strpos($_SERVER['REQUEST_URI'], 'tin-tuc-xe-may') !== false) {
        $news_slug = get_query_var('news_slug');

        if (($news_slug && !is_excluded_news_slug($news_slug, true))) {
            return get_stylesheet_directory() . '/archive-xe-may.php';
        }

        if (is_excluded_news_slug($news_slug, true) && $news_slug) {
            return get_stylesheet_directory() . '/archive-news-motorcycles.php';
        }
    }

    return $template;
}

add_filter('query_vars', 'add_custom_query_vars');
function add_custom_query_vars($vars)
{
    $vars[] = 'make';
    $vars[] = 'model';
    $vars[] = 'section';
    $vars[] = 'filter';
    $vars[] = 'variant_section';
    return $vars;
}

function create_news_post_type()
{
    register_post_type(
        'news',
        array(
            'labels' => array(
                'name' => __('tin-tuc'),
                'singular_name' => __('tin-tuc'),
            ),
            'public' => true,
            'has_archive' => true,
            'rewrite' => array('slug' => 'tin-tuc'),
            'supports' => array('title', 'editor', 'custom-fields'),
        )
    );
}
add_action('init', 'create_news_post_type');

function create_motor_news_post_type()
{
    register_post_type(
        'motorcycle-news',
        array(
            'labels' => array(
                'name' => __('tin-tuc-xe-may'),
                'singular_name' => __('tin-tuc-xe-may'),
            ),
            'public' => true,
            'has_archive' => true,
            'rewrite' => array('slug' => 'tin-tuc-xe-may'),
            'supports' => array('title', 'editor', 'custom-fields'),
        )
    );
}
add_action('init', 'create_motor_news_post_type');

add_filter('post_type_link', function ($post_link, $post) {
    if ($post->post_type === 'listing') {
        $make_id = get_post_meta($post->ID, '_listing_make', true);
        $model_slug = get_post_meta($post->ID, 'listing-model-code', true);
        $make_data = get_term($make_id);

        // Ensure $model_slug is always a string (handle null case)
        $model_slug = $model_slug ?? '';  // If $model_slug is null, set it to an empty string

        $make_name = $make_data->slug;
        if (strpos($model_slug, $make_name . '-') === 0) {
            $model_name = substr($model_slug, strlen($make_name) + 1); // Remove make_slug and hyphen
        } else {
            $model_name = $model_slug; // Fallback to the full model slug if make isn't found
        }
        $make = $make_name;
        $model = $model_name;
        // Replace spaces with dashes for URL compatibility
        $make_slug = sanitize_title($make);
        $model_slug = sanitize_title($model);
        return home_url("xe-oto/$make_slug/$model_slug/");
    }
    if ($post->post_type === 'motorcycle-listing') {
        $make_id = get_post_meta($post->ID, 'make', true);
        $model_slug = get_post_meta($post->ID, 'model_name', true);
        $make_data = get_term($make_id);

        $make_name = $make_data->slug;

        if (strpos($model_slug, $make_name . '-') === 0) {
            $model_name = substr($model_slug, strlen($make_name) + 1); // Remove make_slug and hyphen
        } else {
            $model_name = $model_slug; // Fallback to the full model slug if make isn't found
        }
        $make = $make_name;
        $model = $model_name;
        // Replace spaces with dashes for URL compatibility
        $make_slug = sanitize_title($make);
        $model_slug = sanitize_title($model);
        return home_url("xe-may/$make_slug/$model_slug/");
    }
    return $post_link;
}, 10, 2);

add_action('after_setup_theme', 'enqueue_custom_taxonomy_class'); // Use after_setup_theme
function enqueue_custom_taxonomy_class()
{
    $file_path = get_stylesheet_directory() . '/plugin/wp-cardealer/custom-taxonomy-car-make.php';

    if (file_exists($file_path)) {
        require_once $file_path; // Use require_once to prevent multiple inclusions
    } else {
        error_log("Custom taxonomy class file not found: " . $file_path); // Important for debugging
    }
}

// Function to clear cache when a news post type is added, updated, or deleted
if (is_admin() && strpos($_SERVER['REQUEST_URI'], '/wp-admin/') !== false && !wp_doing_ajax() && !defined('REST_REQUEST')) {
    require_once ABSPATH . 'wp-content/themes/voiture-child/admin-recommend-cars.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/admin/tag-manage.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/admin/gallery.php';
    require_once get_stylesheet_directory() . '/admin/admin-cache.php';
    require_once get_stylesheet_directory() . '/admin/admin-listing-make-evc.php';
    require_once ABSPATH . 'wp-content/themes/voiture-child/admin/admin-recommend-motors.php';

    // Initialize the Custom Taxonomy Car Make class
    if (isset($_GET['taxonomy']) && $_GET['taxonomy'] === 'listing_make' && isset($_GET['post_type']) && $_GET['post_type'] === 'listing') {
        //         require_once get_stylesheet_directory() . '/plugin/wp-cardealer/custom-taxonomy-car-make.php';

        function wp_cardealer_enqueue_media_uploader()
        {
            wp_enqueue_media();
            wp_enqueue_script('wp-cardealer-media-uploader', get_stylesheet_directory_uri() . '/plugin/wp-cardealer/js/car-make.js', array('jquery'), null, true);
        }
        add_action('admin_enqueue_scripts', 'wp_cardealer_enqueue_media_uploader');
    }

    //populate related make values --> FAQs
    function populate_related_make_field($field)
    {
        // Clear any existing choices
        $field['choices'] = array();

        $field['choices'][''] = 'Select Make';

        // Get all terms from the `listing_make` taxonomy
        $terms = get_terms(array(
            'taxonomy' => 'listing_make',
            'hide_empty' => false,
        ));

        // Populate the field choices with terms
        if (!empty($terms) && !is_wp_error($terms)) {
            foreach ($terms as $term) {
                $field['choices'][$term->term_id] = $term->name;
            }
        }

        return $field;
    }
    add_filter('acf/load_field/name=related_make', 'populate_related_make_field');


    //register all tags taxonomy
    function register_all_tags_taxonomy()
    {
        register_taxonomy(
            'all-tags', // Taxonomy name
            'post',     // Post type (or any custom post type)
            [
                'hierarchical' => true, // Allows parent-child relationship
                'labels' => [
                    'name' => 'All Tags',
                    'singular_name' => 'All Tag',
                ],
                'public' => true,
                'show_ui' => true,
                'show_in_menu' => true,
                'show_in_quick_edit' => true,
            ]
        );
    }
    add_action('init', 'register_all_tags_taxonomy');

    //add tags field in news
    add_action('acf/init', 'register_custom_acf_fields');
    function register_custom_acf_fields()
    {
        // Check if ACF is active
        if (function_exists('acf_add_local_field_group')) {

            // Create a field group
            acf_add_local_field_group(array(
                'key' => 'group_tag_management',
                'title' => 'Tags',
                'fields' => array(
                    array(
                        'key' => 'news_tags_dropdown',
                        'label' => 'Select Tags',
                        'name' => 'tags',
                        'type' => 'select',
                        'choices' => array(),
                        'multiple' => true,
                        'allow_null' => true,
                        'ui' => 1,
                        'ajax' => true,
                        'return_format' => 'id',
                    ),
                ),
                'location' => array(
                    array(
                        array(
                            'param' => 'post_type',
                            'operator' => '==',
                            'value' => 'news', // Adjust as necessary for your use case
                        ),
                    ),
                ),
            ));
        }
    }

    // Populate the News ACF 'Select Tags' field dynamically with tag groups and tags
    add_filter('acf/load_field/key=news_tags_dropdown', 'populate_tags_dropdown');

    function populate_tags_dropdown($field)
    {
        // Clear any existing choices
        $field['choices'] = array();


        // Fetch tag groups (parent terms) from the 'all-tags' taxonomy
        $tag_groups = get_terms(array(
            'taxonomy' => 'all-tags',
            'parent' => 0, // Fetch only parent terms (tag groups)
            'hide_empty' => false,
        ));
        if (!empty($tag_groups) && !is_wp_error($tag_groups)) {
            // Loop through each tag group
            foreach ($tag_groups as $tag_group) {
                // Fetch child tags (sub-tags) of the current tag group
                $field['choices'][$tag_group->term_id] = $tag_group->name;
                $tags = get_terms(array(
                    'taxonomy' => 'all-tags',
                    'parent' => $tag_group->term_id, // Get children (tags under the group)
                    'hide_empty' => false,
                ));


                if (!empty($tags) && !is_wp_error($tags)) {
                    // Add tags under the group
                    foreach ($tags as $tag) {
                        $field['choices'][$tag->term_id] = $tag->name;
                    }
                }
            }
        }
        // Return the modified field with populated choices
        return $field;
    }
}
// Custom SEO titles and descriptions based on URL structure
add_filter('pre_get_document_title', 'custom_seo_title');
remove_theme_support('title-tag');

// invalidate listing_type taxonomy cache when new listing_type taxonomy is added
add_action('created_listing_type', 'invalidate_listing_type_cache');
add_action('deleted_listing_type', 'invalidate_listing_type_cache');
add_action('edited_listing_type', 'invalidate_listing_type_cache');

function invalidate_listing_type_cache()
{
    delete_transient('cached_listing_types');
}


add_filter('wpseo_title', 'custom_seo_title', 20);
add_filter('wp_title', 'custom_seo_title', 20);
function custom_seo_title($seo_title)
{
    global $wpdb;
    $is_motorcycle = strpos($_SERVER['REQUEST_URI'], '/xe-may/') !== false;
    $is_car = strpos($_SERVER['REQUEST_URI'], '/xe-oto/') !== false;
    $url_path = $_SERVER['REQUEST_URI'];
    $make = get_query_var('make');
    $model = get_query_var('model');
    $section = get_query_var('section');
    $variant_section = get_query_var('variant_section');
    $news_slug = get_query_var('news_slug');
    $current_year = date('Y');

    // chinese and malay language news article
    $lang = get_query_var('lang');
    $post_name = get_query_var('post_name');
    $post_id = get_query_var('post_id');

    // homepage
    if ($url_path == '/' || $url_path == '') {
        $title = 'Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun';
    }

    // cars for sale
    if ($url_path == '/used-car-market-value-guide') {
        $title = 'Check used car market values in Malaysia free online | WapCar';
    }

    if ($url_path == '/trade-in-your-car') {
        $title = 'Trade-in & Save More on Your Next Car in Malaysia | WapCar';
    }

    // cars
    if ($url_path == '/xe-oto') {
        $title = 'Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun';
    }
    if ($url_path == '/xe-may/') {
        $title = 'Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun';
    }
    // if make && not model
    if ($is_car) {
        if ($make && !$model) {
            $thai_make_name = isset($thai_car_names[$make]) ? $thai_car_names[$make] : $make;

            // Construct the title dynamically
            $title = "Bảng Giá Xe ". ucfirst($make) ." Việt Nam ". $current_year ." - Thông số kỹ thuật, Hình ảnh, Đánh giá, Tin tức | Autofun";
        }
    }

    if ($is_motorcycle) {
        if ($make && !$model) {
            $thai_make_name = isset($thai_car_names[$make]) ? $thai_car_names[$make] : $make;

            // Construct the title dynamically
            $title = "Bảng giá xe máy ". ucfirst($make) ." Việt Nam ". $current_year ." - Thông số kỹ thuật, Hình ảnh, Đánh giá, Tin tức | Autofun";
        }
    }
    // cars filter
    if (strpos($url_path, '/xe-hoi-moi') !== false) {
        $current_url = trim($_SERVER['REQUEST_URI'], '/');
        $last_part = basename($current_url);

        // Remove the "best-" prefix if it exists
        if (strpos($last_part, 'best-') === 0) {
            $last_part = substr($last_part, 5); // Remove the first 5 characters
        }

        $parts = explode('-', $last_part);

        $filters = [];
        $price_range_text = '';

        foreach ($parts as $part) {
            if (strpos($part, 'between') === 0) {
                // Handle price range ("between60to90K")
                $price_range = str_replace(['between', 'to', 'K'], ['', '-', 'K'], $part);
                $price_range_text = "between Triệu $price_range";
            } else {
                // Convert URL-friendly word to sentence-friendly word
                $filters[] = ucfirst($part);
            }
        }

        // Combine filters and price range to form the title
        $filters_text = implode(' ', $filters);
       
        $title = "Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun";
    }

        //motorcycle filter
        if (strpos($url_path, '/xe-may-moi') !== false) {
            $current_url = trim($_SERVER['REQUEST_URI'], '/');
            $last_part = basename($current_url);
    
            // Remove the "best-" prefix if it exists
            if (strpos($last_part, 'best-') === 0) {
                $last_part = substr($last_part, 5); // Remove the first 5 characters
            }
    
            $parts = explode('-', $last_part);
    
            $filters = [];
            $price_range_text = '';
    
            foreach ($parts as $part) {
                if (strpos($part, 'between') === 0) {
                    // Handle price range ("between60to90K")
                    $price_range = str_replace(['between', 'to', 'K'], ['', '-', 'K'], $part);
                    $price_range_text = "between Triệu $price_range";
                } else {
                    // Convert URL-friendly word to sentence-friendly word
                    $filters[] = ucfirst($part);
                }
            }
    
            // Combine filters and price range to form the title
            $filters_text = implode(' ', $filters);
           
            $title = "Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun";
        }

    // individual listing
    if ($make && $model) {
        $make = ucfirst($make);
        $model = ucfirst($model);
        // Check if the URL contains "motorcycles" or "cars"

        if ($is_car) {
            $title = "Giá xe ". $make . " " . $model . " " . $current_year . " - Đánh giá, Thông số kỹ thuật, Hình ảnh, Tin tức | Autofun";
        } elseif ($is_motorcycle) {
            $title = "Giá xe ". $make . " " . $model . " " . $current_year . " - Đánh giá, Thông số kỹ thuật, Hình ảnh, Tin tức | Autofun";
        }

        if ($is_car) {
            switch ($section) {
                case 'tin-tuc':
                    $title = "Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun";
                    break;
                case 'thong-so-ky-thuat':
                    $title = "Thông số kỹ thuật " . $make . " " . $model . " " . $current_year . " - Kích thước, Trọng lượng, Động cơ, Hộp số | Autofun";
                    break;
                case 'hinh-anh':
                    $title = "Hình ảnh Nội & Ngoại thất " . $make . " " . $model . " " . $current_year . " - Thư viện | Autofun";
                    break;
                case 'tieu-hao-nhien-lieu':
                    $title = "Mức Tiêu Hao Nhiên Liệu Của Xe " . $make . " " . $model . " tại Việt Nam | AutoFun";
                    break;

                case 'mau-sac':
                    $title = "Màu xe mới của " . $make . " " . $model . " " . $current_year . ", Kiểm Tra Tất Cả 4 Màu Tại Việt Nam | AutoFun";
                    break;
                default:
                    $title = "Giá xe " . $make . " " . $model . " " . $current_year . " - Đánh giá, Thông số kỹ thuật, Hình ảnh, Tin tức | Autofun";
                    break;
            }
        } elseif ($is_motorcycle) {
            switch ($section) {
                case 'tin-tuc':
                    $title = "Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun";
                    break;
                case 'thong-so-ky-thuat':
                    $title = "Thông số kỹ thuật " . $make . " " . $model . " " . $current_year ." - Kích thước, Trọng lượng, Động cơ, Hộp số | Autofun";
                    break;
                case 'hinh-anh':
                    $title = "Thư viện & Hình ảnh " . $make . " " . $model . " " . $current_year . " | Autofun";
                    break;
                case 'tieu-hao-nhien-lieu':
                    $title = "Tiêu Thụ Nhiên Liệu " . $make . " " . $model . " | Autofun";
                    break;

                case 'mau-sac':
                    $title = "Màu xe mới của " . $make . " " . $model . " " . $current_year .", Kiểm Tra Tất Cả 10 Màu Tại Việt Nam | AutoFun";
                    break;
                default:
                    $title = "Giá xe " . $make . " " . $model . " " . $current_year . " - Đánh giá, Thông số kỹ thuật, Hình ảnh, Tin tức | Autofun";
                    break;
            }
        }
    }


    // individual variant
    if($is_car){
    if ($make && $model && $variant_section) {
        // get post title by post name
        $args = array(
            'name'           => $variant_section,
            'post_type'      => 'variant',
            'post_status'    => 'publish',
            'posts_per_page' => 1
        );
        $query = new WP_Query($args);

        if ($query->have_posts()) {
            $query->the_post();
            $post_title = get_the_title();
            wp_reset_postdata();  // Reset post data

            switch ($section) {
                case 'thong-so-ky-thuat':
                    $title = "Thông số " . $make . " " . $model . " 2023 - Tính năng, Cấu hình động cơ, Kích thước lốp | Autofun" ;
                    break;
                case 'hinh-anh':
                    $title = "Hình ảnh " . $make . " " . $model . " - Ảnh thực HD | Autofun";
                    break;
                default:
                    $title = "Giá xe " . $make . " " . $model . " " . $current_year . " - Khuyến mại, Đánh giá, Thông số, Hình ảnh tại Việt Nam | Autofun";
                    break;
            }
        }
    }
}

if($is_motorcycle){
    if ($make && $model && $variant_section) {
        // get post title by post name
        $args = array(
            'name'           => $variant_section,
            'post_type'      => 'variant',
            'post_status'    => 'publish',
            'posts_per_page' => 1
        );
        $query = new WP_Query($args);

        if ($query->have_posts()) {
            $query->the_post();
            $post_title = get_the_title();
            wp_reset_postdata();  // Reset post data

            switch ($section) {
                case 'thong-so-ky-thuat':
                    $title = "Thông số ".$make." " .$model." - Động cơ, Mức tiêu hao nhiên liệu của, Kích thước lốp | Autofun" ;
                    break;
                case 'hinh-anh':
                    $title = "Hình ảnh ".$make." " .$model." - Ảnh thực HD hình ảnh lớn | Autofun";
                    break;
                default:
                    $title = "Giá xe ".$make ." ".$model." - Khuyến mại, Đánh giá, Thông số, Hình ảnh tại Việt Nam | Autofun";
                    break;
            }
        }
    }
}

  // cars-electric
//   if ($url_path == '/cars-electric') {
//     $title = 'ราคา รถ EV ใน ไทย, EV Cars Thailand | AutoFun';
// }

    /***************** Tools Pages ****************/
    /***************** Tools Pages ****************/
    if ($url_path == '/dung-cu/mua-xe-tra-gop/') {
        $title = 'Mua Xe Trả Góp - Bảng Tính Chi Phí Mua Xe Ô Tô Trả Góp | Autofun';
    }

    if ($url_path == '/dung-cu/bao-hiem-xe') {
        $title = 'Bảo Hiểm Xe - Tính Phí Bảo Hiểm Xe Tại Việt Nam | AutoFun';
    }

    if ($url_path == '/dung-cu/gia-xang-dau') {
        $title = 'Giá Xăng Hôm Nay - Giá Xăng Hiện Tại, RON 92, RON 95, Dầu Diesel, Dầu hỏa ở Việt Nam';
    }

//     if ($url_path == '/tools/fuel-cost-calculator') {
//         $title = 'เครื่องคำนวณค่าน้ำมันเชื้อเพลิง คำนวณค่าน้ำมันเดินทาง ค่าใช้จ่ายน้ำมัน | AutoFun';
//     }

    if ($url_path == '/so-sanh-xe') {
        $title = "Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun";
    }
    if ($url_path == '/so-sanh-xe-may') {
        $title = "So Sánh Xe Hơi và Xe Máy ở Việt Nam";
    }
    // bikes
    if ($url_path == '/dung-cu/mua-xe-tra-gop') {
        $title = 'Mua Xe Trả Góp - Bảng Tính Chi Phí Mua Xe Ô Tô Trả Góp | Autofun';
    }   

    // other pages
    if ($url_path == '/about-us') {
        $title = 'Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun';
    }
    if ($url_path == '/join-us') {
        $title = 'Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun';
    }
    if ($url_path == '/quang-cao-voi-chung-toi') {
        $title = 'Tăng độ tiếp xúc thương hiệu và doanh số cùng Autofun.vn!';
    }

    if ($url_path == '/user-agreement') {
        $title = 'Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun';
    }

    if ($url_path == '/privacy-policy') {
        $title = 'Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun';
    }

    if ($url_path == '/viet-cho-chung-toi') {
        $title = 'Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun';
    }

    // news
    if ((strpos($url_path, '/tin-tuc') !== false && !$news_slug) || $url_path == '/bm' || $url_path == '/zh') {
            $title = "Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun";
    }
    if ($url_path == '/tin-tuc-xe-may') {
        $title = "Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun";
    }
    if ($news_slug) {
        $title = "Tìm giá xe ô tô, Xe máy mới tại Việt Nam, Tin tức, Đánh giá, Hình ảnh | Autofun";
        $news_post = get_news_post_from_news_slug();
        if ($news_post) {
            $title = $news_post->post_title . ' | Autofun';
			
			$yoast_title = get_post_meta($news_post->ID, '_yoast_wpseo_title', true);
            if ($yoast_title) {
                $title = $yoast_title;
            }
        }
    }

    // chinese and malay news article
    if ($lang && $post_name && $post_id) {
        $result = $wpdb->get_var(
            $wpdb->prepare("SELECT news_post_id FROM news_temp WHERE news_id = %d", $post_id)
        );
        $current_post_id = $result ? $result : $post_id;
        $news_post = get_post($current_post_id);
        if ($news_post) {
            $title = $news_post->post_title . ' | Autofun';
        }
    }

    return $title ? $title : $seo_title;
}

add_filter('wpseo_opengraph_url', function () {
    $url_path = $_SERVER['REQUEST_URI'];
    return home_url($url_path);
});

add_action('wp_head', 'add_og_image_to_news');
function add_og_image_to_news()
{
    global $wpdb;

    $news_slug = get_query_var('news_slug');
    $categories = ['latest', 'reviews', 'opinions', 'evs', 'buying-guides', 'owner-stories', 'used-car', 'good-reads', 'culture', 'news', 'car-tips'];

    if ($news_slug && !in_array($news_slug, $categories)) {
        $current_url = $_SERVER['REQUEST_URI'];
        $path_parts = explode('/', trim($current_url, '/'));
        $last_part = end($path_parts);

        // Check if the last part matches the pattern (contains numbers at the end)
        if (preg_match('/(.*)-(\d+)$/', $last_part, $matches)) {
            $news_id = $matches[2];

            $result = $wpdb->get_var(
                $wpdb->prepare("SELECT news_post_id FROM news_temp WHERE news_id = %d", $news_id)
            );
            $current_post_id = $result ? $result : $news_id;

            $image_url = get_image_url($current_post_id);
            echo '<meta property="og:image" content="' . esc_url($image_url) . '" />';
        }
    }
}

add_filter('wpseo_metadesc', function ($description) {
    global $wpdb;

    $url_path = $_SERVER['REQUEST_URI'];
    $make = get_query_var('make');
    $model = get_query_var('model');
    $section = get_query_var('section');
    $variant_section = get_query_var('variant_section');
    $current_year = date('Y');
    $news_slug = get_query_var('news_slug');
	$is_motorcycle = strpos($_SERVER['REQUEST_URI'], '/xe-may/') !== false;
    $is_car = strpos($_SERVER['REQUEST_URI'], '/xe-oto/') !== false;

    // chinese and malay language news article
    $lang = get_query_var('lang');
    $post_name = get_query_var('post_name');
    $post_id = get_query_var('post_id');

    // homepage
    if ($url_path == '/' || $url_path == '') {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ". $current_year ." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";

    }

    // cars
    if ($url_path == '/xe-oto') {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ". $current_year ." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";
    }
    if ($url_path == '/xe-may') {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ". $current_year ." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";
    }
    if ($is_car) {
        if ($make && !$model) {
            $description = "Nhận bảng giá xe ô tô ". ucfirst($make) ." 2022 - 2023 mới nhất tại Việt Nam, tìm các mẫu xe ". ucfirst($make) .", tra xe điện / giá xe ". ucfirst($make) .", thư viện, màu sắc, thông số kỹ thuật, tính năng, đánh giá của chuyên gia, đánh giá của người dùng, hình ảnh và video.";
        }
    }

    if ($is_motorcycle) {
        if ($make && !$model) {
            // Check if the make exists in the Thai car names array
            $thai_make = isset($thai_car_names[$make]) ? $thai_car_names[$make] : $make;

            // Build the description with the dynamically replaced Thai make
            $description = "Nhận bảng giá xe máy / xe tay ga / xe máy điện ". ucfirst($make) ." 2022 - 2023 mới nhất tại Việt Nam, tìm các mẫu xe máy BMW, khảo giá xe máy ". ucfirst($make) .", thư viện hình ảnh, màu sắc, thông số kỹ thuật, tính năng, đánh giá và video của chuyên gia.";
        }
    }


    // cars filter
    if (strpos($url_path, '/xe-hoi-moi') !== false) {
        $current_url = trim($_SERVER['REQUEST_URI'], '/');

        $last_part = basename($current_url);

        // Remove the "best-" prefix if it exists
        if (strpos($last_part, 'best-') === 0) {
            $last_part = substr($last_part, 5); // Remove the first 5 characters
        }

        $parts = explode('-', $last_part);

        $filters = [];
        $price_range_text = '';

        foreach ($parts as $part) {
            if (strpos($part, 'between') === 0) {
                // Handle price range ("between60to90K")
                $price_range = str_replace(['between', 'to', 'K'], ['', '-', 'K'], $part);
                $price_range_text = "between Triệu $price_range";
            } else {
                // Convert URL-friendly word to sentence-friendly word
                $filters[] = ucfirst($part);
            }
        }

        // Combine filters and price range to form the title
        $filters_text = implode(' ', $filters);
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ". $current_year ." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";
    }

    // cars for sale
//     if (strpos($url_path, '/used-car-market-value-guide') !== false) {
//         $description = "Want to buy or sell a second hand car but have no idea about its market value? Get instant car values online with WapCar's free car value calculator. Make it easy to get a great deal.";
//     }

//     if (strpos($url_path, '/trade-in-your-car') !== false) {
//         $description = 'How to trade in your car in Malaysia? WapCar’s online trade-in car calculator creates the best car trade-in value for you, and at the same time solves the problem of when should you trade-in your car (trade in car price) and how to trade in a car with loan, save more on your next car.';
//     }

    // news
    
    if (strpos($url_path, '/tin-tuc') !== false || $url_path == '/bm' || $url_path == '/zh') {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ". $current_year ." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";

    }
    if (strpos($url_path, '/tin-tuc-xe-may') !== false || $url_path == '/bm' || $url_path == '/zh') {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ". $current_year ." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";

    }
    if ($news_slug) {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ". $current_year ." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";

        $news_post = get_news_post_from_news_slug();
        if ($news_post) {
            $description = $news_post->post_excerpt;

            // Get the first sentence
            $sentences = preg_split('/(?<=[.!?])\s+/', $description, 2, PREG_SPLIT_NO_EMPTY);
            $description = $sentences[0] ?? $description;
        }
    }

    // chinese and malay news article
    if ($lang && $post_name && $post_id) {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ". $current_year ." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";


        $result = $wpdb->get_var(
            $wpdb->prepare("SELECT news_post_id FROM news_temp WHERE news_id = %d", $post_id)
        );
        $current_post_id = $result ? $result : $post_id;
        $news_post = get_post($current_post_id);
        if ($news_post) {
            $description = $news_post->post_excerpt;
            $sentences = preg_split('/(?<=[.!?])\s+/', $description, 2, PREG_SPLIT_NO_EMPTY);
            $description = $sentences[0] ?? $description;
        }
    }

    // individual listing
    if ($make && $model) {
        $starting_retail_price = get_starting_retail_price($make, $model);
        $make = ucfirst($make);
        $model = ucfirst($model);
        $listing_post = get_posts(array(
            'name' => $make . '-' . $model,
            'post_type' => 'listing',
            'posts_per_page' => 1
        ));
        $post_id = $listing_post[0]->ID;
        $description = "Giá xe " . $make . " " .  $model .  " mới nhất tại Việt Nam là 545000000 đồng. Xem ngay hướng dẫn mua " . $make . " " .  $model .  " trên Autofun.vn để biết các khuyến mãi, thông số kỹ thuật, tính năng, mức tiêu thụ nhiên liệu của, đánh giá, màu sắc, hình ảnh nội ngoại thất và tin tức của " . $make . " " .  $model .  " ". $current_year .".";


        if ($is_car) {
            switch ($section) {
                case 'tin-tuc':
                    $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ". $current_year ." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";
                    break;
                case 'thong-so-ky-thuat':
                    $description = "Xem đặc điểm và thông số kỹ thuật xe " . $make . " " .  $model .  " tại Việt Nam ". $current_year .", bao gồm mẫu xe " . $make . " " .  $model .  " kích thước D x R x C, trọng lượng, loại động cơ và mô-men xoắn công suất, hộp số, mức tiêu thụ nhiên liệu của, dung tích bình xăng, trang bị an toàn và tính năng tiện nghi.";
                    break;
                case 'hinh-anh':
                    $description = "Xem 22 hình ảnh xe " . $make . " " .  $model ." tại Việt Nam ". $current_year .", Bao gồm 200 ảnh nội thất " . $make . " " .  $model .", 300 ảnh ngoại thất Honda City, cũng như các hình ảnh phía trước và phía sau khác, hình ảnh bên hông, màu sắc và hình ảnh động cơ và khung gầm.";
                    break;
                case 'tieu-hao-nhien-lieu':
                    $description = "Bạn muốn biết về hiệu quả sử dụng nhiên liệu của xe, mỗi 100km sử dụng bao nhiêu xăng? Xem chi tiết mức tiêu hao nhiên liệu của " . $make . " " .  $model ." tại Việt Nam. Kiểm tra dữ liệu trung bình và tính toán xem bạn có thể lái bao nhiêu km với mỗi lít xăng hoặc dầu diesel. Nhận đánh giá của chính chủ về khả năng tiết kiệm xăng của " . $make . " " .  $model .", tìm những chiếc xe tiết kiệm xăng nhất Việt Nam.";
                    break;
                case 'mau-sac':
                    $description = "Xem ảnh các màu Ford Fiesta ". $current_year ." đẹp nhất trong cả 3 màu. Ngoài màu đen, trắng và đỏ cổ điển, mẫu biến thể này còn có các màu sau: , , . Xem hình ảnh của các loại sơn khác nhau và nhận được giá cả.";
                    break;
                default:
                    $description = "Giá xe " . $make . " " .  $model ." mới nhất tại Việt Nam là 5200000000 đồng. Xem ngay hướng dẫn mua " . $make . " " .  $model ." trên Autofun.vn để biết các khuyến mãi, thông số kỹ thuật, tính năng, mức tiêu thụ nhiên liệu của, đánh giá, màu sắc, hình ảnh nội ngoại thất và tin tức của " . $make . " " .  $model ." ". $current_year .".";

                    break;
            }
        }
		
		
        if ($is_motorcycle) {
            switch ($section) {
                case 'tin-tuc':
                    $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ". $current_year ." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";
                    break;
                case 'thong-so-ky-thuat':
                    $description = "Xem đặc điểm và thông số kỹ thuật " . $make . " " .  $model ." tại Việt Nam ". $current_year .", bao gồm mẫu xe máy " . $make . " " .  $model ." kích thước D x R x C, trọng lượng, loại động cơ và mô-men xoắn công suất, hộp số, mức tiêu thụ nhiên liệu của, dung tích bình xăng, trang bị an toàn và tính năng tiện nghi.";
                    break;
                case 'hinh-anh':
                    $description = "Xem 0 hình ảnh xe máy " . $make . " " .  $model ." tại Việt Nam ". $current_year .", Trong cửa hàng đã chụp 200 tấm hình lớn của " . $make . " " .  $model .", và 200 hình chụp các chi tiết về kiểu dáng và màu sắc.";
                    break;
                case 'tieu-hao-nhien-lieu':
                    $description = "Bạn muốn biết về hiệu quả sử dụng nhiên liệu của xe, mỗi 100km sử dụng bao nhiêu xăng? Xem chi tiết mức tiêu hao nhiên liệu của " . $make . " " .  $model ." tại Việt Nam. Kiểm tra dữ liệu trung bình và tính toán xem bạn có thể lái bao nhiêu km với mỗi lít xăng hoặc dầu diesel. Nhận đánh giá của chính chủ về khả năng tiết kiệm xăng của " . $make . " " .  $model .", tìm những chiếc xe tiết kiệm xăng nhất Việt Nam.";
                    break;
                case 'mau-sac':
                    $description = "Xem ảnh các màu Ford Fiesta ". $current_year ." đẹp nhất trong cả 3 màu. Ngoài màu đen, trắng và đỏ cổ điển, mẫu biến thể này còn có các màu sau: , , . Xem hình ảnh của các loại sơn khác nhau và nhận được giá cả.";
                    break;
                default:
                    $description = "Giá xe máy BMW F 800 ". $current_year ." mới tại Việt Nam bắt giá từ 0 đồng. Xem ngay bảng giá lăn bánh xe điện, xe tay ga BMW F 800 mới nhất, các đánh giá, mẫu xe, thông số kỹ thuật, tính năng, mức tiêu thụ nhiên liệu của, hình ảnh, màu sắc, khuyến mãi và tin tức.";

                    break;
            }
        }
    }

    $sections = ['overview', 'news', 'specs', 'gallery', 'fuel-consumption', 'colors', ''];
    if ($make && $model && !in_array($section, $sections) && !$variant_section) {
        $args = array(
            'name'           => $section,
            'post_type'      => 'variant',
            'post_status'    => 'publish',
            'posts_per_page' => 1
        );
        $query = new WP_Query($args);

        if ($query->have_posts()) {
            $query->the_post();
            $post_title = get_the_title();
            wp_reset_postdata();

            $description = "Giá xe " . $make . " " .  $model .  " mới nhất tại Việt Nam là 545000000 đồng. Xem ngay hướng dẫn mua " . $make . " " .  $model .  " trên Autofun.vn để biết các khuyến mãi, thông số kỹ thuật, tính năng, mức tiêu thụ nhiên liệu của, đánh giá, màu sắc, hình ảnh nội ngoại thất và tin tức của " . $make . " " .  $model .  " ". $current_year .".";

        }
    }

    // individual variant
    if($is_car){
    if ($make && $model && $variant_section) {
        // get post title by post name
        $args = array(
            'name'           => $variant_section,
            'post_type'      => 'variant',
            'post_status'    => 'publish',
            'posts_per_page' => 1
        );
        $query = new WP_Query($args);

        if ($query->have_posts()) {
            $query->the_post();
            $post_title = get_the_title();
            wp_reset_postdata();

            $listing_name = ucfirst($make) . ' ' . ucfirst($model);
            $description = "Giá xe ".$listing_name." mới nhất tại Việt Nam là 500 Triệu đồng. Xem ngay bài hướng dẫn mua xe ".$listing_name." trên Autofun.vn để biết các thông tin khuyến mãi ".$listing_name." năm ".$current_year.", thông số kỹ thuật, tính năng, mức tiêu thụ nhiên liệu của, đánh giá, màu sắc, hình ảnh nội ngoại thất, tin tức và thông tin.";

            switch ($section) {
                case 'thong-so-ky-thuat':
                    $description = "Xem cấu hình, chức năng và thông số kỹ thuật ".$listing_name." mới nhất tại Việt Nam, bao gồm kích thước ".$listing_name." D x R x C, chiều dài cơ sở, dung tích bình nhiên liệu, cỡ lốp, cỡ vành, trọng lượng, dung tích động cơ, công suất cực đại và tiêu thụ nhiên liệu kết hợp và thiết bị an toàn.";
                    break;
                case 'hinh-anh':
                    $description = "Xem những hình ảnh chụp thực tế HD ".$listing_name." mới nhất tại Việt Nam. Các bạn có thể xem hình ảnh nội ngoại thất ".$listing_name." tại Autofun.vn, bao gồm động cơ, thân trước, thân sau, nóc xe, gầm xe, lốp và gương chiếu hậu, nắp thùng, đèn hậu, màu sắc ".$listing_name.".";
                    break;
                default:
                $description = "Giá xe ".$listing_name." mới nhất tại Việt Nam là 500 Triệu đồng. Xem ngay bài hướng dẫn mua xe ".$listing_name." trên Autofun.vn để biết các thông tin khuyến mãi ".$listing_name." năm ".$current_year.", thông số kỹ thuật, tính năng, mức tiêu thụ nhiên liệu của, đánh giá, màu sắc, hình ảnh nội ngoại thất, tin tức và thông tin.";

                    break;
            }
        }
    }
}
    // individual variant
    if($is_motorcycle){
        if ($make && $model && $variant_section) {
            // get post title by post name
            $args = array(
                'name'           => $variant_section,
                'post_type'      => 'motorcycle-variant',
                'post_status'    => 'publish',
                'posts_per_page' => 1
            );
            $query = new WP_Query($args);
    
            if ($query->have_posts()) {
                $query->the_post();
                $post_title = get_the_title();
                wp_reset_postdata();
    
                $listing_name = ucfirst($make) . ' ' . ucfirst($model);
                $description = "Giá xe máy ". $listing_name." mới nhất tại Việt Nam là 34,943 Triệu đồng. Xem ngay bài hướng dẫn mua xe máy ". $listing_name." trên Autofun.vn để biết các thông tin khuyến mãi ". $listing_name." năm ".$current_year.", thông số kỹ thuật, tính năng, mức tiêu thụ nhiên liệu của, đánh giá, màu sắc, hình ảnh nội ngoại thất, tin tức và thông tin.";
    
                switch ($section) {
                    case 'thong-so-ky-thuat':
                        $description = "Cùng xem cấu hình, tính năng, thông số kỹ thuật xe máy ". $listing_name." mới nhất Việt Nam, bao gồm công suất động cơ ". $listing_name.", cỡ lốp, mức tiêu hao nhiên liệu của, dung tích bình xăng, trọng lượng bản thân, kích thước D x R x C và các trang bị an toàn.";
                        break;
                    case 'hinh-anh':
                        $description = "Xem những hình ảnh chụp thực tế HD của xe máy ". $listing_name." mới nhất tại Việt Nam. Các bạn có thể xem hình ngoại thất ". $listing_name." trên Autofun.vn, bao gồm động cơ, lốp xe, thân trước, thân xe, đệm ngồi, đuôi xe, chiếu hậu gương và đèn pha, đèn bên, đèn hậu và màu sắc ". $listing_name.".";
                        break;
                    default:
                    $description = "Giá xe máy ". $listing_name." mới nhất tại Việt Nam là 34,943 Triệu đồng. Xem ngay bài hướng dẫn mua xe máy ". $listing_name." trên Autofun.vn để biết các thông tin khuyến mãi ". $listing_name." năm ".$current_year.", thông số kỹ thuật, tính năng, mức tiêu thụ nhiên liệu của, đánh giá, màu sắc, hình ảnh nội ngoại thất, tin tức và thông tin.";
    
                        break;
                }
            }
        }
    }
    // cars-electric
//     if ($url_path == '/cars-electric') {
//         $description = "รถ EV ใน ไทย EV Cars Thailand (BEV/HEV/PHEV/FCEV) ในประเทศไทย รวบรวมรถไฟฟ้าทุกยี่ห้อในโลก ราคา รถ EVใหม่และใช้แล้วที่ถูกที่สุด ข่าวการเปิดตัวรถยนต์ EV ใหม่ สถานีชาร์จ ประกันภัย สินเชื่อ ซ่อมแซม บำรุงรักษา ภาษีถนน โปรโมชั่น และนโยบายสิทธิพิเศษอื่นๆ สำหรับรถยนต์ไฟฟ้า";
//     }

    /****************** Tools Pages ****************/

    if ($url_path == '/dung-cu/mua-xe-tra-gop') {
        $description = "Chọn xe muốn mua, tự động tính toán chi phí mua xe trả góp, tính lãi suất vay mua xe hàng tháng, dễ dàng đơn giản có được kế hoạch trả góp hàng tháng chính xác.";
    }

//     if ($url_path == '/tools/road-tax-calculator') {
//         $description = "กำลังค้นหาเครื่องมือคำนวณภาษีรถยนต์？ เช็คภาษีรถยนต์ของรถยนต์ใหม่2020-2021หรือรถมือสองที่AutoFunได้";
//     }

    if ($url_path == '/dung-cu/bao-hiem-xe') {
        $description = "Bảng tính phí Bảo hiểm Xe ô tô Trực tuyến tại Việt Nam. Tính toán chi phí mua bảo hiểm cho ô tô của bạn.";
    }

//     if ($url_path == '/tools/fuel-cost-calculator') {
//         $description = "เครื่องคำนวณค่าน้ำมันเชื้อเพลิง วิธีคำนวณค่าน้ำมันรถยนต์ คำนวณค่าใช้จ่ายน้ำมันเชื้อเพลิงในการเดินทางตามรุ่นรถยนต์ อัตราการใช้น้ำมันเชื้อเพลิง ระยะทาง ราคาน้ำมัน และคำนวณค่าน้ำมันเชื้อเพลิงต่อปี";
//     }
	
	    if ($url_path == '/dung-cu/mua-xe-tra-gop') {
        $description = "Chọn xe muốn mua, tự động tính toán chi phí mua xe trả góp, tính lãi suất vay mua xe hàng tháng, dễ dàng đơn giản có được kế hoạch trả góp hàng tháng chính xác.";
    }
	
    if ($url_path == '/so-sanh-xe') {
        $description = 'Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam '.$current_year.' trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động. ';
    }

    if ($url_path == '/so-sanh-xe-may') {
        $description = 'Bạn đang muốn so sánh xe hơi và xe máy ở Việt Nam? Đừng tìm nữa, hãy truy cập vào AutoFunVN! Trang web toàn diện của chúng tôi cho phép bạn dễ dàng so sánh thông số kỹ thuật, giá cả và tính năng của nhiều loại xe khác nhau, giúp bạn đưa ra quyết định mua hàng thông minh. Bắt đầu tìm kiếm của bạn ngay hôm nay và tìm thấy chiếc xe hơi hoặc xe máy hoàn hảo cho nhu cầu của bạn!';
    }
    
    if ($url_path == '/dung-cu/gia-xang-dau') {
        $oil_price_string = get_oil_prices();
        $description = 'Giá Xăng Dầu Hôm Nay - Giá xăng RON 92, RON 95, Dầu KO, DO 0 mới nhất ở Việt Nam: RON 92 giá 19.400 đ 1 lít, RON 95 giá 20.500 đ 1 lít, Dầu KO giá 18.830 đ 1 lít, Dầu DO 0 giá 18.140 đ 1 lít.';
    }

	
	
	
    // other pages
    if ($url_path == '/about-us') {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ".$current_year." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";
    }

    if ($url_path == '/quang-cao-voi-chung-toi') {
        $description = "Autofun.vn là chuyên trang nội dung về Ô tô - Xe máy hàng đầu tại Việt Nam. Chúng tôi cung cấp nội dung và các giải pháp quảng cáo hiệu quả để giúp bạn tiếp cận đến khách hàng mục tiêu và đạt được doanh số mong muốn.";
    }

    if ($url_path == '/user-agreement') {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ".$current_year." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";
    }
    if ($url_path == '/join-us') {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ".$current_year." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";
    }
    if ($url_path == '/privacy-policy') {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ".$current_year." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";
    }

    if ($url_path == '/write-for-us') {
        $description = "Xem các thông tin liên quan đến ô tô, xe máy mới nhất Việt Nam ".$current_year." trên Autofun.vn, bao gồm tin tức xe, giá xe ô tô, hình ảnh, thông số kỹ thuật, video, đánh giá, so sánh xếp hạng, hướng dẫn mua hàng, v.v, theo dõi thời gian thực về ô tô mới và xe máy mới được sắp được tung ra hoặc niêm yết trên thị trường năng động.";
    }

    return $description;
});

function format_number_with_commas($number)
{
    $number = floatval($number);

    return number_format($number, 0, '.', ',');
}

function get_starting_retail_price($make, $model)
{
    $listing_name = $make . '-' . $model;

    $listing_posts = get_posts(array(
        'name' => $listing_name,
        'post_type' => 'listing',
        'posts_per_page' => 1
    ));
    $listing_post = $listing_posts[0];
    if ($listing_post) {
        $variants = get_posts(array(
            'post_type' => 'variant',
            'posts_per_page' => -1,
            'post_parent' => $listing_post->ID
        ));

        $starting_retail_price = 0;
        if ($variants) {
            foreach ($variants as $variant) {
                $retail_price = get_post_meta($variant->ID, 'retail_price', true);
                $retail_price = (int) $retail_price;

                if ($retail_price < $starting_retail_price || $starting_retail_price == 0) {
                    $starting_retail_price = $retail_price;
                }
            }
        }
    }

    return $starting_retail_price;
}


function usermenu_rewrite_rules()
{
    /** remember to update the page id in production */
    add_rewrite_rule(
        '^car-owner-service/?$',
        'index.php?page_id=726337',
        'top'
    );
}
add_action('init', 'usermenu_rewrite_rules');

function custom_cars_for_sale_rewrite_rules()
{
    add_rewrite_rule(
        '^(cars-for-sale|used-cars-for-sale|recon-cars-for-sale)/?$',
        // Replace with the Elementor page ID
        'index.php?page_id=728046',
        'top'
    );
}
add_action('init', 'custom_cars_for_sale_rewrite_rules');


add_filter('wpseo_robots', function ($robots) {
    return 'noindex, nofollow';
});

add_filter('wpseo_robots', function ($robots) {
    if (is_singular('news') && get_post_status() !== 'publish') {
        return 'noindex, nofollow';
    }
    return $robots;
});

// no index url like /listing-make/bmw
add_filter('wpseo_robots', function ($robots) {
    $url_path = $_SERVER['REQUEST_URI'];

    if (strpos($url_path, '/listing-make') !== false) {
        return 'noindex, nofollow';
    }

    return $robots;
});


// custom login logo
function custom_login_logo()
{
    echo '<style type="text/css">
        #login h1 a, .login h1 a {
 			background-image: url(https://static.wapcar.my/pc/my/images/eb54a8b59d0291d638c3.svg);
            height: 100px; /* Change the height as needed */
            width: 100%; /* Use 100% width for responsiveness */
            background-size: contain; /* Adjust this property as needed */
        }
    </style>';
}

add_action('login_enqueue_scripts', 'custom_login_logo');

function custom_login_logo_url()
{
    return home_url();
}
add_filter('login_headerurl', 'custom_login_logo_url');

function custom_login_logo_url_title()
{
    return 'Go to WapCar Homepage';
}

function redirect_to_custom_404_page()
{
    if (is_404()) {
        wp_redirect(home_url('/page-not-found/'));
        exit;
    }
}
add_action('template_redirect', 'redirect_to_custom_404_page');

// contact7 form plugin
add_filter('wpcf7_form_tag', function ($tag) {
    if (!empty($_GET[$tag['name']])) {
        $tag['values'] = [sanitize_text_field($_GET[$tag['name']])];
    }
    return $tag;
});

function create_book_test_drive_table()
{
    global $wpdb;
    $table_name = $wpdb->prefix . 'book_test_drive_requests';

    // Check if the table already exists
    if (get_option('book_test_drive_table_created')) {
        return;
    }

    $charset_collate = $wpdb->get_charset_collate();

    $sql = "CREATE TABLE $table_name (
        id mediumint(9) NOT NULL AUTO_INCREMENT,
        name varchar(100) NOT NULL,
        email varchar(100) NOT NULL,
        phone varchar(20) NOT NULL,
        make varchar(100) NOT NULL,
        model varchar(100) NOT NULL,
        created_at datetime DEFAULT CURRENT_TIMESTAMP NOT NULL,
        PRIMARY KEY (id)
    ) $charset_collate;";

    require_once(ABSPATH . 'wp-admin/includes/upgrade.php');
    dbDelta($sql);

    // Mark as created
    update_option('book_test_drive_table_created', true);
}

// Hook to run during theme setup
add_action('init', 'create_book_test_drive_table');

function save_test_drive_request($contact_form)
{
    $submission = WPCF7_Submission::get_instance();
    if ($submission) {
        $posted_data = $submission->get_posted_data();

        $name = sanitize_text_field($posted_data['your-name']);
        $email = sanitize_email($posted_data['your-email']);
        $phone = sanitize_text_field($posted_data['number-193']);
        $make = sanitize_text_field($posted_data['make']);
        $model = sanitize_text_field($posted_data['model']);

        global $wpdb;
        $table_name = $wpdb->prefix . 'book_test_drive_requests';

        $wpdb->insert(
            $table_name,
            [
                'name' => $name,
                'email' => $email,
                'phone' => $phone,
                'make' => $make,
                'model' => $model,
                'created_at' => current_time('mysql')
            ]
        );

        wp_redirect(home_url());
    }
}
add_action('wpcf7_before_send_mail', 'save_test_drive_request');


// advertise with us page
function inline_smooth_scroll_script()
{
    ?>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const button = document.getElementById('contact-us-button');
            const targetSection = document.getElementById('contact-us-form');

            if (button && targetSection) {
                button.addEventListener('click', function(e) {
                    e.preventDefault();
                    targetSection.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });
                });
            }
        });
    </script>
    <?php
}
add_action('wp_footer', 'inline_smooth_scroll_script');

// Add Shortcode
function custom_box_shortcode($atts, $content = null)
{
    // Attributes
    $atts = shortcode_atts(
        array(
            'width' => '100%',     // Default width
            'height' => '300px',   // Default height
        ),
        $atts
    );

    // Return HTML
    return '<div style="width: ' . esc_attr($atts['width']) . '; height: ' . esc_attr($atts['height']) . '; border: 1px solid #ccc; padding: 10px; box-sizing: border-box;">' .
        do_shortcode($content) .
        '</div>';
}
add_shortcode('custom_box', 'custom_box_shortcode');

function generate_custom_url_shortcode($atts)
{
    // Extract the attributes passed to the shortcode
    $atts = shortcode_atts(
        array(
            'path' => '', // Default path is empty
        ),
        $atts,
        'custom_url'
    );

    // Get the home URL and append the provided path
    $base_url = home_url();
    $full_url = trailingslashit($base_url) . ltrim($atts['path'], '/');

    // Return the generated URL
    return esc_url($full_url);
}

// Register the shortcode [custom_url path="your-path"]
add_shortcode('custom_url', 'generate_custom_url_shortcode');

//short code for sitemap news
function display_sitemap_news_shortcode()
{
    // Query arguments
    $args = array(
        'post_type'      => 'news',
        'posts_per_page' => 20,
        'meta_query' => array(
            'relation' => 'AND',
            array(
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '=='
            ),
            array(
                'key'     => 'publish_time',
                'value'   => current_time('mysql'),
                'compare' => '<',
                'type'    => 'DATETIME'
            )
        ),
        'meta_key'       => 'publish_time',
        'orderby'        => 'meta_value',
        'order'          => 'DESC',
        'meta_type'      => 'DATETIME',
    );

    $query = new WP_Query($args);
    if ($query->have_posts()) {
        $latest_news = [];
        while ($query->have_posts()) : $query->the_post();
            $news_id = get_the_ID();
            $title = get_the_title($news_id);

            $latest_news[] = [
                'id' => $news_id,
                'title' => $title,
                'link'  => get_custom_post_link($news_id),
            ];
        endwhile;
    }
    // Start output buffer
    ob_start();

    if (!empty($latest_news)) {
        echo '<h2 class="sitemap-news-heading"> Latest News </h2>';
        echo '<div class="latest-news-grid" >';

        foreach ($latest_news as $post_data) {
            echo '<div class="latest-news-item">';
            echo '<a href="' . esc_url($post_data['link']) . '" >' . esc_html($post_data['title']) . '</a>';
            echo '</div>';
        }

        echo '</div>';
    } else {
        echo '<p>' . esc_html__('', 'text-domain') . '</p>';
    }

    // Reset post data
    wp_reset_postdata();

    // Return the output
    return ob_get_clean();
}
add_shortcode('sitemap_news', 'display_sitemap_news_shortcode');

//shortcode for brands
function display_sitemap_brands_shortcode()
{
    $brands = new WP_Term_Query(array(
        'taxonomy'   => 'listing_make',
        'hide_empty' => false, // Include terms without posts
        'orderby'    => 'name', // Order alphabetically by name
        'order'      => 'ASC',  // Ascending order
        'meta_query' => array(
            array(
                'key'   => 'state', // The meta key
                'value' => '1',     // The value you're filtering for
                'compare' => '='    // Comparison operator
            ),
        ),
    ));

    // Start output buffer
    ob_start();

    if (!empty($brands->terms)) {
        echo '<h2 class="sitemap-news-heading"> Popular Car Brands in Malaysia </h2>';
        echo '<div class="news-grid" >';

        foreach ($brands->terms as $brand) {
            echo '<div class="news-item">';
            echo '<a href="' . esc_url(home_url('/cars/' . $brand->slug)) . '" >' . esc_html($brand->name) . '</a>';
            echo '</div>';
        }

        echo '</div>';
    } else {
        echo '<p>' . esc_html__('No brands found.', 'text-domain') . '</p>';
    }

    // Reset post data
    wp_reset_postdata();

    // Return the output
    return ob_get_clean();
}
add_shortcode('sitemap_brands', 'display_sitemap_brands_shortcode');

//shortcode for popular models
function display_sitemap_popular_models_shortcode()
{
    $recommend_car_models = get_option('recommended_car_models');
    $car_models_data = maybe_unserialize($recommend_car_models);

    $popular_cars_ids = [];

    if (!empty($car_models_data) && is_array($car_models_data)) {
        foreach ($car_models_data as $category => $category_data) {

            if (isset($category_data['car_models']) && is_array($category_data['car_models'])) {
                foreach ($category_data['car_models'] as $model) {
                    if (is_array($model) && isset($model['id'], $model['type'], $model['sort'])) {
                        // Collect the models where type = 1
                        if ($model['type'] == 1) {
                            $popular_cars_data[] = $model;
                        }
                    }
                }
            }
        }
    }

    // Sort the popular cars data by the 'sort' field in ascending order
    usort($popular_cars_data, function ($a, $b) {
        return $a['sort'] <=> $b['sort']; // Ascending order, including 0
    });

    $popular_cars_ids = array_column($popular_cars_data, 'id');

    $popular_models = [];
    if (!empty($popular_cars_ids)) {
        // Fetch popular car posts
        $popular_cars_query = new WP_Query(array(
            'post_type' => 'listing',
            'posts_per_page' => -1,
            'post__in' => $popular_cars_ids,
            'orderby' => 'post__in',
        ));

        if ($popular_cars_query->have_posts()) {
            while ($popular_cars_query->have_posts()) {
                $popular_cars_query->the_post();
                $popular_models[] = [
                    'id' => get_the_ID(),
                    'name' => get_the_title(),
                    'link' => get_permalink(),
                ];
            }
        }
        // Reset post data after the query
        wp_reset_postdata();
    }
    // Start output buffer
    ob_start();

    if (!empty($popular_models)) {
        echo '<h2 class="sitemap-news-heading"> Popular Models </h2>';
        echo '<div class="news-grid" >';

        foreach ($popular_models as $models) {
            echo '<div class="news-item">';
            echo '<a href="' . esc_url($models['link']) . '" >' . esc_html($models['name']) . '</a>';
            echo '</div>';
        }

        echo '</div>';
    } else {
        echo '<p>' . esc_html__('No models found.', 'text-domain') . '</p>';
    }

    // Reset post data
    wp_reset_postdata();

    // Return the output
    return ob_get_clean();
}
add_shortcode('sitemap_popular_models', 'display_sitemap_popular_models_shortcode');

// Add the "Picture" column to the admin list for the banner post type
function add_picture_column($columns)
{
    // Create a new array of columns, placing the "Picture" column right after the "Title"
    $new_columns = array();

    // Loop through columns to add them to the new array
    foreach ($columns as $key => $column) {
        $new_columns[$key] = $column;

        // Check if it's the 'title' column and add 'picture' after it
        if ($key == 'title') {
            $new_columns['picture'] = 'Picture'; // Add the "Picture" column after the title
        }
    }

    return $new_columns;
}
add_filter('manage_banner_posts_columns', 'add_picture_column');

// Display the picture field content in the new column
function display_picture_column($column, $post_id)
{
    if ($column == 'picture') {
        // Retrieve the ACF image field value
        $banner_image = get_field('picture', $post_id); // 'picture' is the ACF field name

        if (!empty($banner_image['ID'])) {
            // Fetch the attachment post using the image ID
            $image_post = get_post($banner_image['ID']);
            $image_guid = !empty($image_post) ? $image_post->guid : ''; // Get the GUID

            if ($image_guid) {
                // Display the image using the GUID
                echo '<img src="' . esc_url($image_guid) . '" alt="' . esc_attr($banner_image['alt']) . '" style="max-width: 100px; height: auto;">';
            } else {
                echo 'No image';
            }
        } else {
            echo 'No image';
        }
    }
}
add_action('manage_banner_posts_custom_column', 'display_picture_column', 10, 2);

function display_current_page_url()
{
    $news_id = get_last_numeric_id_from_url();
    $post;
    if ($news_id != NULL) {
        global $wpdb;
        $result = $wpdb->get_var(
            $wpdb->prepare("SELECT news_post_id FROM news_temp WHERE news_id = %d", $news_id)
        );
        $post = get_post($result);
    } else {
        // Get the current URL path
        $current_url = trim(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH), '/');
        // Extract the slug (last part of the URL)
        $slug = basename($current_url); // This gets the last segment of the URL
        // Get the post ID based on the slug and post type
        $post = get_page_by_path($slug, OBJECT, 'news'); // Replace 'news' with your custom post type
    }


    if ($post) {
        $related_model_ids = get_post_meta($post->ID, 'related_car_model', true);
        $make = '';
        $model = '';
        $model_data = '';
        if (!empty($related_model_ids)) {
            if (!is_array($related_model_ids)) {
                $related_model_ids = explode(',', $related_model_ids);
            }
            $found_model = false;
            foreach ($related_model_ids as $model_id) {
                if ($found_model) {
                    continue;
                }
                $model_id = intval(trim($model_id));
                $model_post = get_post($model_id);
                if ($model_post && !is_wp_error($model_post)) {
                    $make_id = get_post_meta($model_id, '_listing_make', true);
                    if ($make_id) {
                        $make_term = get_term($make_id);
                        if ($make_term && !is_wp_error($make_term)) {
                            $make_slug = $make_term->slug;
                            $full_slug = $model_post->post_name;
                            $model = trim(str_replace($make_slug, '', $full_slug));
                            $model_name = ltrim($model, '-');
                            $model_data .= '[individual_listing_tabs make="' . esc_attr(strtolower(strtolower($make_slug))) . '" model="' . esc_attr(strtolower(strtolower($model_name))) . '" 											selected_tab="Tin tức"]';
                            $found_model = true;
                        }
                    }
                }
            }
        }
        // Get author details and publish date with time
        $author_id = $post->post_author;
        $author_name = get_the_author_meta('display_name', $author_id);
        $publish_date = get_the_date('M j, Y h:i A', $post); // Format date to match the design
        $author_image_url = get_the_author_meta('user_url', $author_id);
        // Start output buffering
        ob_start();

        // Display post title, author avatar, author name, publish date, and content with inline CSS
    ?>
        <section>
            <div class="related-tabs">
                <?php echo do_shortcode($model_data); ?>
            </div>
        </section>
    <?php
    }
}
add_shortcode('current_url', 'display_current_page_url');

function create_historical_oil_price_table()
{
    global $wpdb;

    // Check if the table has already been created
    if (get_option('historical_oil_price_table_created')) {
        return; // Exit if the table already exists
    }

    $table_name = $wpdb->prefix . 'historical_oil_price';
    $charset_collate = $wpdb->get_charset_collate();

    $sql = "CREATE TABLE $table_name (
        id bigint(20) NOT NULL AUTO_INCREMENT,
        post_id bigint(20) NOT NULL,
        oil_name varchar(100) NOT NULL,
        price decimal(10,2) NOT NULL,
        start_date date NOT NULL,
        end_date date NOT NULL,
        created_at datetime DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY  (id),
        KEY post_id (post_id),
        KEY oil_name (oil_name)
    ) $charset_collate;";

    // Include WordPress upgrade functions
    require_once(ABSPATH . 'wp-admin/includes/upgrade.php');
    dbDelta($sql);

    // Check for errors and log them
    if ($wpdb->last_error) {
        error_log('Table creation error: ' . $wpdb->last_error);
    } else {
        // Mark the table as created in the WordPress options table
        update_option('historical_oil_price_table_created', true);
    }
}
add_action('init', 'create_historical_oil_price_table');


// Function to store historical data when oil post is updated
function store_historical_oil_price($post_id)
{
    if (defined('DOING_AUTOSAVE') && DOING_AUTOSAVE) {
        return;
    }

    if (get_post_type($post_id) !== 'oil') {
        return;
    }

    $oil_name = get_field('oil_name', $post_id);
    $price = get_field('oil_price', $post_id);
    $start_date = get_field('start_date', $post_id);
    $end_date = get_field('end_date', $post_id);

    if (empty($oil_name) || empty($price) || empty($start_date) || empty($end_date)) {
        return;
    }

    // Convert the date format from m/d/Y to Y-m-d
    $start_date = DateTime::createFromFormat('d/m/Y', $start_date);
    $end_date = DateTime::createFromFormat('d/m/Y', $end_date);

    if (!$start_date || !$end_date) {
        return;
    }

    // Format the date in Y-m-d format
    $start_date = $start_date->format('Y-m-d');
    $end_date = $end_date->format('Y-m-d');

    global $wpdb;
    $table_name = $wpdb->prefix . 'historical_oil_price';

    $result = $wpdb->insert(
        $table_name,
        array(
            'post_id' => $post_id,
            'oil_name' => $oil_name,
            'price' => $price,
            'start_date' => $start_date,
            'end_date' => $end_date
        ),
        array(
            '%d',
            '%s',
            '%f',
            '%s',
            '%s'
        )
    );

    if ($result === false) {
        error_log('Database insertion failed: ' . $wpdb->last_error);
    }
}

function acf_date_format_callback($value, $post_id, $field)
{
    if (!empty($value) && !strtotime($value)) {
        return null;
    }
    return $value;
}
add_filter('acf/update_value/name=start_date', 'acf_date_format_callback', 10, 3);
add_filter('acf/update_value/name=end_date', 'acf_date_format_callback', 10, 3);
add_action('acf/save_post', 'store_historical_oil_price', 20);

function custom_compare_cars_rewrite_rule()
{
    add_rewrite_rule(
        '^compare-cars/([^/]+)-vs-([^/]+)/?$',
        'index.php?pagename=compare-cars&car1=$matches[1]&car2=$matches[2]',
        'top'
    );
}
add_action('init', 'custom_compare_cars_rewrite_rule');

function get_custom_post_link($post_id, $default_link = '')
{
    global $wpdb;
    $table_name = 'news_temp';

    // Check if the post_id exists in the table
    $result = $wpdb->get_var(
        $wpdb->prepare("SELECT news_id FROM $table_name WHERE news_post_id = %d", $post_id)
    );
    // Use the existing post ID if found, otherwise use the default post ID
    $final_post_id = $result ?? $post_id;

    // get second language of post
    $second_lang = get_post_meta($post_id, 'second_language', true);
    if ($second_lang === 'my-zh') {
        $post_name = get_post_field('post_name', $post_id);
        $custom_link = home_url() . '/zh/' . $post_name . '-' . $final_post_id;
        return $custom_link;
    } elseif ($second_lang === 'my-my') {
        $post_name = get_post_field('post_name', $post_id);
        $custom_link = home_url() . '/bm/' . $post_name . '-' . $final_post_id;
        return $custom_link;
    }

    // Append the ID at the end of the permalink
    $custom_link = untrailingslashit(get_permalink($post_id)) . '-' . $final_post_id;

    return $custom_link;
}

// Remove last / from all URl 
add_filter('user_trailingslashit', function ($url, $type) {
    return rtrim($url, '/');
}, 10, 2);

//Get the News Id 
function get_last_numeric_id_from_url()
{
    // // Get the current URL path
    $current_url = trim(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH), '/');

    // Parse the URL and get the path
    $path = parse_url($current_url, PHP_URL_PATH);

    // Get the last part of the URL path
    $last_part = basename($path);

    // Match the ID at the end of the last part (digits after the last hyphen)
    if (preg_match('/-(\d+)$/', $last_part, $matches)) {
        return $matches[1]; // Return the ID
    }

    // Return null if no ID is found
    return null;
}

// Get standard data and time for malayasia
function convert_myt_to_ist($malaysian_time)
{
    // Create a DateTime object with the Malaysia Time (MYT) timezone
    $date = new DateTime($malaysian_time, new DateTimeZone('Asia/Kuala_Lumpur'));

    // Convert to Indian Standard Time (IST), which is 2.5 hours behind MYT
    $date->setTimezone(new DateTimeZone('Asia/Kolkata'));

    // Return the formatted time as 'Nov 29, 2024 09:15 AM'
    return $date->format('M d, Y h:i A');
}

//css for the admin listing make description
function custom_admin_styles_and_scripts()
{
    echo '<style>
        body.wp-admin .column-description {
            width: 60% !important;
            overflow: hidden;
            display: block; /* Ensure block-level rendering */
            position: relative;
            text-overflow: ellipsis;
        }
    </style>';
    echo '<script>
        document.addEventListener("DOMContentLoaded", function () {
            const descriptions = document.querySelectorAll(".column-description");
            descriptions.forEach(function (description) {
                const lineHeight = parseFloat(window.getComputedStyle(description).lineHeight);
                const maxHeight = lineHeight * 4; // 2 lines
                if (description.offsetHeight > maxHeight) {
                    let originalText = description.textContent.trim();
                    while (description.scrollHeight > maxHeight) {
                        description.textContent = description.textContent.slice(0, -1).trim();
                    }
                    description.textContent = description.textContent.trim() + "...";
                }
            });
        });
    </script>';
}
add_action('admin_head', 'custom_admin_styles_and_scripts');

function wapcar_author_query_vars($vars)
{
    $vars[] = 'author_slug';
    return $vars;
}
add_filter('query_vars', 'wapcar_author_query_vars');

function get_custom_author_post_link($author_id, $default_link)
{
    global $wpdb;
    $table_name = 'author_temp';

    // Check if the author_id exists in the table
    $user_id = $wpdb->get_var(
        $wpdb->prepare("SELECT user_id FROM $table_name WHERE wp_user_id = %d", $author_id)
    );

    // If no user_id is found, return the original default link
    if (!$user_id) {
        return $default_link;
    }

    // Parse the default author URL
    $parsed_url = parse_url($default_link);

    // Replace the last segment (author slug) with the user_id
    $path_segments = explode('/', trim($parsed_url['path'], '/'));
    $path_segments[count($path_segments) - 1] = $user_id; // Replace the author slug with user_id
    $parsed_url['path'] = '/' . implode('/', $path_segments);

    // Rebuild the URL with the modified path
    $custom_link = (isset($parsed_url['scheme']) ? $parsed_url['scheme'] . '://' : '') .
        (isset($parsed_url['host']) ? $parsed_url['host'] : '') .
        $parsed_url['path'];

    return $custom_link;
}

function get_wp_user_id_from_custom_author_id($author_id)
{
    global $wpdb;

    if (!$author_id) {
        return null; // Return null if no author ID is provided.
    }

    // Fetch wp_user_id from the author_temp table.
    return $wpdb->get_var(
        $wpdb->prepare("SELECT wp_user_id FROM author_temp WHERE user_id = %d", $author_id)
    );
}

// Used to check the language in news page
function get_category_status()
{
    // List of allowed categories
    $menu_categories = [
        'latest',
        'good-reads',
        'opinions',
        'evs',
        'buying-guides',
        'owner-stories',
        'used-cars'
    ];

    // Get the current URL path
    $current_url = $_SERVER['REQUEST_URI']; // Get URL path after the domain
    $url_parts = explode('/', trim($current_url, '/')); // Break into parts, trim slashes

    // Check if the third segment (category) matches a menu category
    if (isset($url_parts[1]) && in_array($url_parts[1], $menu_categories)) {
        return true;
    }

    // Default response if no match
    return false;
}

// get image url
function get_image_url($id, $image_type = 'post')
{
    if ($image_type == 'author') {
        $author_id = $id;
        $author_image_url = get_the_author_meta('user_url', $author_id);
        if ($author_image_url) {
            return $author_image_url;
        }
        return 'https://storage.googleapis.com/wp-my/malaysia/2025/01/13003120/author-placeholder.jpg';
    } elseif ($image_type == 'post') {
        $post_thumbnail_id = get_post_thumbnail_id($id);
        $thumbnail_post = get_post($post_thumbnail_id);
        $guid = $thumbnail_post->guid;

        return $guid;
    }

    return 'https://storage.googleapis.com/wp-my/malaysia/2025/01/13004236/image_placeholder.png';
}

// rss.xml
add_action('init', function () {
    add_rewrite_rule(
        '^sitemap/rss\.xml$',
        'index.php?custom_rss_sitemap=1',
        'top'
    );
});

add_filter('query_vars', function ($vars) {
    $vars[] = 'custom_rss_sitemap';
    return $vars;
});

add_action('template_redirect', function () {
    if (get_query_var('custom_rss_sitemap')) {
        generate_custom_rss_feed();
        exit;
    }
});

function generate_custom_rss_feed()
{
    header('Content-Type: application/rss+xml; charset=UTF-8');

    $current_time = gmdate('D, d M Y H:i:s') . ' GMT';
    $site_url = home_url();
    $rss_url = $site_url . '/sitemap/rss.xml';

    echo '<?xml version="1.0" encoding="UTF-8"?>';
    echo '<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:wfw="http://wellformedweb.org/CommentAPI/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:sy="http://purl.org/rss/1.0/modules/syndication/" xmlns:slash="http://purl.org/rss/1.0/modules/slash/" xmlns:georss="http://www.georss.org/georss" xmlns:geo="http://www.w3.org/2003/01/geo/wgs84_pos#">';
    echo '<channel>';
    echo '<title>WapCar Automotive News</title>';
    echo '<link>' . esc_url($site_url) . '</link>';
    echo '<atom:link href="' . esc_url($rss_url) . '" rel="self" type="application/rss+xml"/>';
    echo '<description>Rss feed from WapCar Automotive News in Malaysia</description>';
    echo '<language>en-US</language>';
    echo '<sy:updatePeriod>hourly</sy:updatePeriod>';
    echo '<sy:updateFrequency>1</sy:updateFrequency>';
    echo '<pubDate>' . esc_html($current_time) . '</pubDate>';
    echo '<copyright>2025 WapCar</copyright>';
    echo '<lastBuildDate>' . esc_html($current_time) . '</lastBuildDate>';
    echo '</channel>';
    echo '</rss>';
}

add_action('send_headers', function () {
    // Remove default cache headers
    header_remove('Pragma');
    header_remove('Cache-Control');
    header_remove('Expires');

    // Add custom cache headers
    header('Cache-Control: public, max-age=3600');
});


// news dashboard filters

add_action('restrict_manage_posts', function () {
    global $typenow;
    global $wpdb;

    // Check if we're on the news post type
    if ($typenow !== 'news') {
        return;
    }

    // Get all authors who have written news posts
    $authors = $wpdb->get_results("
        SELECT DISTINCT u.ID, u.display_name 
        FROM {$wpdb->users} u 
        INNER JOIN {$wpdb->posts} p ON u.ID = p.post_author 
        WHERE p.post_type = 'news' 
        AND p.post_status != 'trash'
        ORDER BY u.display_name
    ");

    // Get currently selected author
    $selected = isset($_GET['author']) ? (int)$_GET['author'] : 0;

    // Get current ID search value
    $id_search = isset($_GET['exact_id']) ? (int)$_GET['exact_id'] : '';

    // Add ID search box
    ?>
    <input
        type="number"
        name="exact_id"
        value="<?php echo esc_attr($id_search); ?>"
        placeholder="<?php _e('Search by exact ID', 'your-text-domain'); ?>"
        style="width: 150px; margin-right: 6px;">

    <!-- Author dropdown -->
    <select name="author" id="filter-by-author">
        <option value=""><?php _e('All Authors', 'your-text-domain'); ?></option>
        <?php foreach ($authors as $author): ?>
            <option value="<?php echo esc_attr($author->ID); ?>" <?php selected($selected, $author->ID); ?>>
                <?php echo esc_html($author->display_name); ?> (ID: <?php echo esc_html($author->ID); ?>)
            </option>
        <?php endforeach; ?>
    </select>
<?php
});

// Modify the query based on the selected filters
add_action('pre_get_posts', function ($query) {
    if (!is_admin() || !$query->is_main_query()) {
        return;
    }

    if ($query->get('post_type') !== 'news') {
        return;
    }

    // Handle exact ID search
    if (isset($_GET['exact_id']) && !empty($_GET['exact_id'])) {
        $exact_id = (int)$_GET['exact_id'];
        if ($exact_id > 0) {
            $query->set('p', $exact_id);
            // When searching by exact ID, we don't need other filters
            return;
        }
    }

    // Handle author filter if no exact ID is specified
    if (isset($_GET['author']) && !empty($_GET['author'])) {
        $author_id = (int)$_GET['author'];
        if ($author_id > 0) {
            $query->set('author', $author_id);
        }
    }
});

function custom_ivory_search_placeholder($form)
{
    // Replace 'Search...' with your custom placeholder text
    $form = str_replace('placeholder="Search here..."', 'placeholder="Search for news by title"', $form);
    return $form;
}
add_filter('get_search_form', 'custom_ivory_search_placeholder');

add_filter('wpseo_opengraph_title', 'custom_seo_title', 20);
function news_site_name($site_name)
{
    // Check if we're on the news archive page
    if (is_post_type_archive('news') || is_tax('news-category')) {
        return 'WapCar News'; // Return the custom site name for news page
    }

    // Return the default site name for other pages
    return 'Wapcar';
}
add_filter('wpseo_opengraph_site_name', 'news_site_name', 20);

add_filter('wpseo_opengraph_url', function () {
    $url_path = $_SERVER['REQUEST_URI'];
    return home_url($url_path);
});

function news_site_name_and_og_metadata()
{

    $url_path = $_SERVER['REQUEST_URI'];
    if (strpos($url_path, '/news') !== false) {
        global $post;
        $published_time = get_the_date('c', $post); // ISO8601 format
        $modified_time = get_the_modified_date('c', $post); // ISO8601 format
        echo '<meta property="article:section" content="Cars" />' . PHP_EOL;
        echo '<meta property="article:published_time" content="' . esc_attr($published_time) . '" />' . PHP_EOL;
        echo '<meta property="article:modified_time" content="' . esc_attr($modified_time) . '" />' . PHP_EOL;
        echo '<meta property="og:updated_time" content="' . esc_attr($modified_time) . '" />' . PHP_EOL;
    }
}
add_action('wp_head', 'news_site_name_and_og_metadata');

add_action('send_headers', function () {
    header("Cache-Control: no-cache, no-store, must-revalidate, max-age=0");
    header("Pragma: no-cache");
    header("Expires: 0");
});

function get_listing_from_query_vars()
{
    global $wpdb;
    // Get make & model from query vars
    $make  = get_query_var('make');
    $model = get_query_var('model');

    if (empty($make) || empty($model)) {
        return [];
    }

    // Generate a unique cache key
    $cache_key = 'listing_post_' . $make . '_' . $model;

    // Try getting cached data from WordPress Transients
    $cached_data = get_transient($cache_key);
    if ($cached_data) {
        return $cached_data;
    }



    // Set transient to "loading" to prevent duplicate queries
    // set_transient($cache_key, 'loading', 5 * MINUTE_IN_SECONDS);


    $post_title = $make . "-" . $model;
    $listing_post = get_posts(array(
        'name' => $post_title,
        'post_type' => 'listing',
        'posts_per_page' => 1,
    ));

    if (empty($listing_post)) {
        set_transient($cache_key, null, 12 * HOUR_IN_SECONDS);

        return null;
    }

    $post = $listing_post[0];

    $post_thumbnail_id = get_post_thumbnail_id($post->ID);
    $thumbnail_post = get_post($post_thumbnail_id);
    $guid = $thumbnail_post->guid;

    $post_meta = get_post_meta($post->ID);
    $listing_make_id = $post_meta['_listing_make'][0];
    $listing_make_term = get_term($listing_make_id);

    $listing_post_response = [
        'post' => $post,
        'post_meta' => $post_meta,
        'listing_make_term' => $listing_make_term,
        'thumbnail' => $guid
    ];

    // get all posts of type variant and have post parent as the listing post
    $variant_args = array(
        'post_type' => 'variant',
        'post_parent' => $post->ID,
        'posts_per_page' => -1,
        // 'meta_query' => array(
        //     array(
        //         'key' => 'state',
        //         'value' => '1',
        //         'compare' => '=',
        //     )
        // ),
    );
    $variant_posts = get_posts($variant_args);
    $listing_post_response['variant_posts'] = $variant_posts;
    $variant_post_ids = array_map(function ($variant_post) {
        return $variant_post->ID;
    }, $variant_posts);
    $variant_post_ids = array_map('intval', $variant_post_ids);
    $placeholders = implode(',', array_fill(0, count($variant_post_ids), '%d'));

    // get post meta of all variant posts in a single query
    if (!empty($variant_post_ids)) {
        $variant_post_meta_query = $wpdb->prepare(
            "SELECT post_id, meta_key, meta_value FROM $wpdb->postmeta WHERE post_id IN ($placeholders)",
            ...$variant_post_ids
        );
        $all_variant_post_meta_rows = $wpdb->get_results($variant_post_meta_query);

        $variant_meta_data = [];
        $prices = [];
        $min_price = null;
        $max_price = null;
        foreach ($all_variant_post_meta_rows as $row) {
            $variant_meta_data[$row->post_id][$row->meta_key][] = maybe_unserialize($row->meta_value);

            if ($row->meta_key === 'retail_price') {
                $price = maybe_unserialize($row->meta_value);
                $price = intval(str_replace(',', '', $price));
                if ($price > 0) {
                    $prices[] = $price;
                }
            }
        }

        if (!empty($prices)) {
            $min_price = min($prices);
            $max_price = max($prices);
        }

        // price details
        $listing_post_response['min_price'] = $min_price;
        $listing_post_response['max_price'] = $max_price;
        if ($min_price && $max_price) {
            if ($min_price == $max_price) {
                $price = 'THB ' . number_format($min_price);
            } else {
                $price = 'THB ' . number_format($min_price) . ' - THB ' . number_format($max_price);
            }
        } else {
            $price = 'ยังไม่คอนเฟิร์ม';
        }
        $listing_post_response['price'] = $price;
        $listing_post_response['variant_meta_data'] = $variant_meta_data;
    }

    // get image data of all variant posts in a single query
    $image_data = get_variant_image_data($variant_posts);
    $listing_post_response['image_data'] = $image_data;

    set_transient($cache_key, $listing_post_response, 12 * HOUR_IN_SECONDS);

    return $listing_post_response;
}

function get_variant_image_data($variant_posts)
{
    global $wpdb;

    if (empty($variant_posts)) {
        return [];
    }
    $variant_ids = wp_list_pluck($variant_posts, 'ID');

    $placeholders = implode(',', array_fill(0, count($variant_ids), '%d'));

    $sql = $wpdb->prepare(
        "SELECT colour, type, image_data FROM car_image WHERE variant_post_id IN ($placeholders)",
        ...$variant_ids
    );

    $data = $wpdb->get_results($sql);

    return $data;
}

function get_variant_from_query_vars()
{
    global $wpdb;
    $make = get_query_var('make');
    $model = get_query_var('model');
    $section = get_query_var('section');
    $variant_section = get_query_var('variant_section');

    $listing_name = $make . '-' . $model;

    $individual_pages = ['overview', 'news', 'specs', 'gallery', 'fuel-consumption', 'colors'];
    if (!in_array($section, $individual_pages)) {
        $variant_post = get_posts(array(
            'name' => $section,
            'post_type' => 'variant',
            'posts_per_page' => 1
        ));
        if ($variant_post) {
            $section = 'overview';
            $variant_section = $variant_post[0]->post_name;
        }
    }

    // Generate a unique cache key
    $cache_key = 'variant_post_' . $make . '_' . $model . '_' . $variant_section;

    // Try getting cached data from WordPress Transients
    $cached_data = get_transient($cache_key);
    if ($cached_data) {
        return $cached_data;
    }

    // Set transient to "loading" to prevent duplicate queries
    // set_transient($cache_key, 'loading', 5 * MINUTE_IN_SECONDS);

    $listing_post = get_posts(array(
        'name' => $listing_name,
        'post_type' => 'listing',
        'posts_per_page' => 1,
    ));
    $listing_post = $listing_post[0];

    if (!$listing_post) {
        return false;
    }

    $variant_posts = get_posts(array(
        'post_type' => 'variant',
        'posts_per_page' => 1,
        'name' => $variant_section
    ));

    if (!$variant_posts) {
        return 'No variant found.';
    }
    $variant_post = $variant_posts[0];
    $variant_post_meta = get_post_meta($variant_post->ID);

    $post_thumbnail = get_post_thumbnail_id($variant_post->ID);
    $thumbnail_post = get_post($post_thumbnail);
    $variant_image_url = isset($thumbnail_post->guid) ? esc_url($thumbnail_post->guid) : '';

    $variant_post_response = [
        'listing_post' => $listing_post,
        'variant_post' => $variant_post,
        'variant_post_meta' => $variant_post_meta,
        'variant_image_url' => $variant_image_url
    ];

    // get image data later

    set_transient($cache_key, $variant_post_response, 12 * HOUR_IN_SECONDS);

    return $variant_post_response;
}

function block_bingbot()
{
    if (strpos($_SERVER['HTTP_USER_AGENT'], 'bingbot') !== false) {
        wp_die('Access Denied');
    }
}
add_action('init', 'block_bingbot');

function custom_news_post_slug($permalink, $post)
{
    if ($post->post_type == 'news' && is_admin()) {
        $permalink = 'test'; //get_custom_post_link($post->ID);
    }
    return $permalink;
}
// add_filter('post_link', 'custom_news_post_slug', 1000, 2);


function modify_admin_post_list_links($actions, $post)
{
    if ($post->post_type === 'news') {
        $post_link = get_permalink($post->ID);
        $custom_link = get_custom_post_link($post->ID, $post_link);

        // Update the "View" link only for published posts
        if ($post->post_status === 'publish' && isset($actions['view'])) {
            $actions['view'] = '<a href="' . esc_url($custom_link) . '" target="_blank">View</a>';
        }
    }
    return $actions;
}
add_filter('post_row_actions', 'modify_admin_post_list_links', 10, 2);

function custom_preview_link_with_id($preview_link, $post)
{
    if ($post->post_type === 'news') {
        if ($post->post_status === 'publish') {
            $post_link = get_permalink($post->ID);
            $custom_preview_link = get_custom_post_link($post->ID, $post_link);
            return $custom_preview_link;
        }
    }
    return $preview_link; // Return the original preview link if it's not a news post
}
add_filter('preview_post_link', 'custom_preview_link_with_id', 10, 2);


// Append post ID to the permalink in the admin news edit page
add_filter('get_sample_permalink_html', 'append_post_id_to_news_permalink', 10, 4);

function append_post_id_to_news_permalink($permalink, $post_id, $new_title, $new_slug)
{
    if (get_post_type($post_id) === 'news') {
        // Provide the custom permalink format with post ID but keep it editable
        $updated_permalink = get_custom_post_link($post_id, $permalink);
        return sprintf(
            __('Permalink: %s'),
            '<a href="' . esc_url($updated_permalink) . '" target="_blank">' . $updated_permalink . '</a>'
        );
    }

    return $permalink;
}

// Update the "View Post" link in the success message
add_filter('post_updated_messages', 'customize_post_updated_messages');

function customize_post_updated_messages($messages)
{
    global $post;

    // Check if the post type is 'news'
    if (!$post || $post->post_type !== 'news') {
        return $messages;
    }

    $post_id = $post->ID;
    $permalink = get_permalink($post_id);

    // Modify permalink to include the post ID in the URL
    $updated_permalink = get_custom_post_link($post_id, $permalink);

    // Update the success messages for saving or publishing posts
    $messages['news'][1] = sprintf(
        'Post updated. <a href="%s">View Post</a>',
        esc_url($updated_permalink)
    );
    $messages['news'][6] = sprintf(
        'Post published. <a href="%s">View Post</a>',
        esc_url($updated_permalink)
    );

    return $messages;
}

// Customize Yoast sitemap URL for news, variant, and listing post types
add_filter('wpseo_xml_sitemap_post_url', 'customize_yoast_sitemap_post_urls', 10, 2);
function customize_yoast_sitemap_post_urls($url, $post)
{
    // Check for specific post types
    if ($post->post_type === 'news') {
        // Append the post ID with a hyphen at the end of the URL
        $url = get_custom_post_link($post->ID, $url);
    } elseif ($post->post_type === 'variant') {
        // Get the model associated with the variant
        $model_id = $post->post_parent;
        $model_name = get_the_title($model_id);

        // Get the make term ID directly from the model post
        $make_term_id = get_post_meta($model_id, '_listing_make', true);
        $make_slug = get_term_slug_by_id($make_term_id);

        // Sanitize slugs
        $model_slug = sanitize_title($model_name); // Get the slug for the model
        $model_slug = str_replace($make_slug, '', $model_slug);
        $model_slug = trim($model_slug, '-');

        $variant_slug = $post->post_name;

        if ($make_slug && $model_slug && $variant_slug) {
            // Construct the URL: cars/{make}/{model}/{variant}
            $url = home_url("cars/" . sanitize_title($make_slug) . "/" . $model_slug . "/" . $variant_slug);
        }
    }

    return $url;
}

function exclude_unpublished_news_from_sitemap($excluded_posts)
{
    $args = array(
        'post_type'      => 'news',
        'post_status'    => array('draft', 'pending', 'private', 'trash'), // Exclude all non-published statuses
        'fields'         => 'ids',
        'posts_per_page' => -1,
    );

    $unpublished_posts = get_posts($args);

    return array_merge($excluded_posts, $unpublished_posts);
}
add_filter('wpseo_exclude_from_sitemap_by_post_ids', 'exclude_unpublished_news_from_sitemap');

// Helper function to retrieve the term slug by ID
function get_term_slug_by_id($term_id)
{
    if (!$term_id) {
        return '';
    }
    $term = get_term($term_id, 'listing_make');
    return $term && !is_wp_error($term) ? $term->slug : '';
}

// Modify author URL in the sitemap
add_filter('wpseo_sitemap_entry', function ($url, $type, $object) {
    if ($type === 'user') {
        $author_id = $object->ID;
        if (isset($author_id)) {
            $original_url = isset($url['loc']) ? $url['loc'] : ''; // Access the 'loc' key

            if (!empty($original_url)) {
                $custom_author_link = get_custom_author_post_link($author_id, $original_url);

                // Update the 'loc' key with the new custom author link
                $url['loc'] = $custom_author_link;
            }
        }
    }
    return $url;
}, 10, 3);


// Add to sitemap index (same as before)
add_filter('wpseo_sitemap_index', 'add_listing_make_to_sitemap');
function add_listing_make_to_sitemap($sitemap_index)
{
    if (taxonomy_exists('listing_make')) {
        $taxonomy_sitemap_url = home_url('/make-sitemap.xml');
        $sitemap_index .= '<sitemap><loc>' . esc_url($taxonomy_sitemap_url) . '</loc></sitemap>';
    }
    return $sitemap_index;
}

// // No .htaccess needed - Handle routing with 'init' and 'parse_request'
// add_action( 'init', 'add_listing_make_rewrite_rule' );
// function add_listing_make_rewrite_rule() {
//     add_rewrite_tag('%listing_make_sitemap%', '([^/]+)'); // Still needed for query var
// }


add_action('parse_request', 'generate_listing_make_sitemap');
function generate_listing_make_sitemap($wp)
{
    if ($wp->request === 'make-sitemap.xml') {
        header('Content-Type: application/xml; charset=UTF-8');
        echo '<?xml version="1.0" encoding="UTF-8"?>';
        echo '<?xml-stylesheet type="text/xsl" href="' . esc_url(home_url('/main-sitemap.xsl')) . '"?>';
        echo '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
                     xmlns:image="http://www.google.com/schemas/sitemap-image/1.1"
                     xmlns:xhtml="http://www.w3.org/1999/xhtml"
                     xmlns:news="http://www.google.com/schemas/sitemap-news/0.9"
                     xmlns:video="http://www.google.com/schemas/sitemap-video/1.1">';

        $terms = get_terms(array(
            'taxonomy' => 'listing_make',
            'hide_empty' => false,
        ));

        if (! is_wp_error($terms) && ! empty($terms)) {
            foreach ($terms as $term) {
                $make_slug = $term->slug; // Get the slug of the make
                $custom_url = home_url('/cars/' . $make_slug . '/'); // Format: car/make-name

                echo '<url>';
                echo '<loc>' . esc_url($custom_url) . '</loc>';
                echo '<lastmod>' . date('c', time()) . '</lastmod>'; // Use current timestamp
                echo '<changefreq>weekly</changefreq>';
                echo '<priority>0.8</priority>';
                echo '</url>';
            }
        }

        echo '</urlset>';
        exit;
    }
}

add_filter('xmlsf_url', 'modify_xml_sitemap_url', 10, 2);
function modify_xml_sitemap_url($url, $post)
{
    if ($post->post_type === 'news') {
        $url = get_custom_post_link($post->ID, $url);
    }
    return $url;
}


// Disable the publish time field on edit
function disable_publish_time_on_edit($field)
{
    global $post;

    if ($post && $post->ID) { // If post exists (editing mode)
        $field['disabled'] = true;
    }

    return $field;
}
add_filter('acf/load_field/name=publish_time', 'disable_publish_time_on_edit');

function restrict_past_datetime()
{
?>
    <script>
        jQuery(document).ready(function($) {
            let field = $('input[name="acf[field_XXXXXX]"]'); // Replace with your actual ACF field key

            if (field.length) {
                let now = new Date();
                let year = now.getFullYear();
                let month = ('0' + (now.getMonth() + 1)).slice(-2);
                let day = ('0' + now.getDate()).slice(-2);
                let hours = ('0' + now.getHours()).slice(-2);
                let minutes = ('0' + now.getMinutes()).slice(-2);

                let minDateTime = `${year}-${month}-${day}T${hours}:${minutes}`;
                field.attr('min', minDateTime); // Restrict past date & time

                field.on('change', function() {
                    if ($(this).val() < minDateTime) {
                        alert("You cannot select a past date/time!");
                        $(this).val(minDateTime);
                    }
                });
            }
        });
    </script>
<?php
}
add_action('admin_footer', 'restrict_past_datetime');


function set_publish_time_on_publish($new_status, $old_status, $post)
{
    // Ensure this runs only for posts (change 'post' to your post type if needed)
    if ($post->post_type !== 'news') {
        return;
    }

    // If transitioning from 'draft', 'pending', or 'auto-draft' to 'publish'
    if (in_array($old_status, ['draft', 'pending', 'auto-draft']) && $new_status === 'publish') {
        $current_time = current_time('Y-m-d H:i'); // Get WordPress time
        update_field('publish_time', $current_time, $post->ID);
    }
}
add_action('transition_post_status', 'set_publish_time_on_publish', 10, 3);

function get_news_post_from_news_slug()
{
    global $wpdb;
    $news_slug = get_query_var('news_slug');
    $categories = ['latest', 'reviews', 'opinions', 'evs', 'buying-guides', 'owner-stories', 'used-car', 'good-reads', 'culture', 'news', 'car-tips'];

    if ($news_slug && !in_array($news_slug, $categories)) {
        $current_url = $_SERVER['REQUEST_URI'];
        $path_parts = explode('/', trim($current_url, '/'));
        $last_part = end($path_parts);

        // Check if the last part matches the pattern (contains numbers at the end)
        if (preg_match('/(.*)-(\d+)$/', $last_part, $matches)) {
            $news_id = $matches[2];

            $result = $wpdb->get_var(
                $wpdb->prepare("SELECT news_post_id FROM news_temp WHERE news_id = %d", $news_id)
            );
            $current_post_id = $result ? $result : $news_id;

            $news_post = get_post($current_post_id);

            if ($news_post) {
                return $news_post;
            }
        }
    }

    return null;
}

function custom_enqueue_scripts()
{
    // Enqueue the parent theme's styles (if not already loaded)
    wp_enqueue_style('parent-style', get_template_directory_uri() . '/style.css');

    // Enqueue your custom JS file (with the correct path)
    wp_enqueue_script(
        'custom-toggle-js', // Handle for the script
        get_stylesheet_directory_uri() . '/headers/js/toggle-switch.js', // Correct path to your JS file
        array(), // Dependencies (leave empty if none)
        null, // Version (null means it will not add a version query string)
        true // Load script in footer (set to false if you need it in the header)
    );
}

function get_motor_listing_from_query_vars()
{
    global $wpdb;
    // Get make & model from query vars
    $make  = get_query_var('make');
    $model = get_query_var('model');

    if (empty($make) || empty($model)) {
        return [];
    }

    // Generate a unique cache key
    $cache_key = 'motor_listing_post_' . $make . '_' . $model;

    // Try getting cached data from WordPress Transients
    $cached_data = get_transient($cache_key);
//     if ($cached_data) {
//         return $cached_data;
//     }



    // Set transient to "loading" to prevent duplicate queries
    // set_transient($cache_key, 'loading', 5 * MINUTE_IN_SECONDS);


    $post_title = $make . "-" . $model;
    $listing_post = get_posts(array(
        'name' => $post_title,
        'post_type' => 'motorcycle-listing',
        'posts_per_page' => 1,
    ));

    if (empty($listing_post)) {
        set_transient($cache_key, null, 12 * HOUR_IN_SECONDS);

        return null;
    }

    $post = $listing_post[0];

    $post_thumbnail_id = get_post_thumbnail_id($post->ID);
    $thumbnail_post = get_post($post_thumbnail_id);
    $guid = $thumbnail_post->guid;

    $post_meta = get_post_meta($post->ID);
    $listing_make_id = $post_meta['make'][0];
    $listing_make_term = get_term($listing_make_id);

    $listing_post_response = [
        'post' => $post,
        'post_meta' => $post_meta,
        'listing_make_term' => $listing_make_term,
        'thumbnail' => $guid
    ];

    // get all posts of type variant and have post parent as the listing post
    $variant_args = array(
        'post_type' => 'motorcycle-variant',
        'post_parent' => $post->ID,
        'posts_per_page' => -1,
        // 'meta_query' => array(
        //     array(
        //         'key' => 'state',
        //         'value' => '1',
        //         'compare' => '=',
        //     )
        // ),
    );
    $variant_posts = get_posts($variant_args);

    $listing_post_response['variant_posts'] = $variant_posts;

    $variant_post_ids = array_map(function ($variant_post) {
        return $variant_post->ID;
    }, $variant_posts);

    $variant_post_ids = array_map('intval', $variant_post_ids);

    $placeholders = implode(',', array_fill(0, count($variant_post_ids), '%d'));

    // get post meta of all variant posts in a single query
    if (!empty($variant_post_ids)) {
        $variant_post_meta_query = $wpdb->prepare(
            "SELECT post_id, meta_key, meta_value FROM $wpdb->postmeta WHERE post_id IN ($placeholders)",
            ...$variant_post_ids
        );
        $all_variant_post_meta_rows = $wpdb->get_results($variant_post_meta_query);

        $variant_meta_data = [];
        $prices = [];
        $min_price = null;
        $max_price = null;

        foreach ($all_variant_post_meta_rows as $row) {
            $variant_meta_data[$row->post_id][$row->meta_key][] = maybe_unserialize($row->meta_value);

            if ($row->meta_key === 'price') {
                $price = maybe_unserialize($row->meta_value);
                $price = intval(str_replace(',', '', $price));
                if ($price > 0) {
                    $prices[] = $price;
                }
            }
        }

        if (!empty($prices)) {
            $min_price = min($prices);
            $max_price = max($prices);
        }

        // price details
        $listing_post_response['min_price'] = $min_price;
        $listing_post_response['max_price'] = $max_price;
        if ($min_price && $max_price) {
            if ($min_price == $max_price) {
                $price = 'THB  ' . number_format($min_price);
            } else {
                $price = 'THB  ' . number_format($min_price) . ' - THB  ' . number_format($max_price);
            }
        } else {
            $price = 'ยังไม่คอนเฟิร์ม';
        }
        $listing_post_response['price'] = $price;
        $listing_post_response['variant_meta_data'] = $variant_meta_data;
    }

    // get image data of all variant posts in a single query
    $image_data = get_variant_image_data($variant_posts);
    $listing_post_response['image_data'] = $image_data;

    set_transient($cache_key, $listing_post_response, 12 * HOUR_IN_SECONDS);

    return $listing_post_response;
}

function get_motor_variant_from_query_vars()
{
    global $wpdb;
    $make = get_query_var('make');
    $model = get_query_var('model');
    $section = get_query_var('section');
    $variant_section = get_query_var('variant_section');

    $listing_name = $make . '-' . $model;

    $individual_pages = ['overview', 'news', 'specs', 'gallery', 'fuel-consumption', 'colors'];
    if (!in_array($section, $individual_pages)) {
        $variant_post = get_posts(array(
            'name' => $section,
            'post_type' => 'motorcycle-variant',
            'posts_per_page' => 1
        ));
        if ($variant_post) {
            $section = 'overview';
            $variant_section = $variant_post[0]->post_name;
        }
    }

    // Generate a unique cache key
    $cache_key = 'variant_post_' . $make . '_' . $model . '_' . $variant_section;

    // Try getting cached data from WordPress Transients
    // $cached_data = get_transient($cache_key);
    // if ($cached_data) {
    //     return $cached_data;
    // }

    // Set transient to "loading" to prevent duplicate queries
    // set_transient($cache_key, 'loading', 5 * MINUTE_IN_SECONDS);

    $listing_post = get_posts(array(
        'name' => $model,
        'post_type' => 'motorcycle-listing',
        'posts_per_page' => 1,
    ));

    $listing_post = $listing_post[0];

    if (!$listing_post) {
        return false;
    }

    $variant_posts = get_posts(array(
        'post_type' => 'motorcycle-variant',
        'posts_per_page' => 1,
        'name' => $variant_section
    ));

    if (!$variant_posts) {
        return 'No variant found.';
    }
    $variant_post = $variant_posts[0];
    $variant_post_meta = get_post_meta($variant_post->ID);

    $post_thumbnail = get_post_thumbnail_id($variant_post->ID);
    $thumbnail_post = get_post($post_thumbnail);
    $variant_image_url = isset($thumbnail_post->guid) ? esc_url($thumbnail_post->guid) : '';

    $variant_post_response = [
        'listing_post' => $listing_post,
        'variant_post' => $variant_post,
        'variant_post_meta' => $variant_post_meta,
        'variant_image_url' => $variant_image_url
    ];

    // get image data later

    set_transient($cache_key, $variant_post_response, 12 * HOUR_IN_SECONDS);

    return $variant_post_response;
}

function motor_latest_news_shortcode($atts)
{
$translate = [
    'Latest News' => 'ข่าวล่าสุด',
];
    $all_news = get_latest_news_data('motorcycle-news');
    $news = array_slice($all_news, 0, 5);
?>
    <!-- Your HTML Structure for Related News -->
    <h2 style="margin-bottom: 16px; " class="wa-title-text"> <?php echo $translate['Latest News']; ?></h2>
    <div id="buying-guide-news-container" style="list-style: none; padding: 0;">
        <?php foreach ($all_news as $news): ?>
            <?php
            $title = $news['title'];
            $guid = $news['thumbnail_url'];
            $custom_link = $news['link'];
            $author_name = $news['author'];
            $post_date = $news['post_date'];
            ?>
            <div class="buying-guide-news-item">
                <!-- Post Thumbnail -->
                <div class="news-thumbnail">
                    <a href="<?php echo $custom_link; ?>">
                        <img src="<?php echo esc_url($guid); ?>" alt="<?php echo $title; ?>" style="width: 110px; border-radius: 5px; height:74px;">
                    </a>
                </div>
                <!-- Post Info -->
                <div class="news-info" style="flex: 1;">
                    <h3 style="font-size: 14px; font-weight: bold; color: #262626; margin: 0; overflow: hidden; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;font-weight:700;">
                        <a href="<?php echo $custom_link; ?>" style="color: inherit; text-decoration: none;"><?php echo $title; ?></a>
                    </h3>

                    <div class="news-meta-author-page">
                        <?php echo $author_name; ?> • <?php echo $post_date; ?>
                    </div>
                </div>
            </div>
        <?php endforeach; ?>
    </div>

    <!-- "View More" Button -->
    <div id="view-more-container" style="text-align: center; margin-top: 20px;">
        <a href="<?php echo home_url('/news-motorcycles/latest'); ?>" class="view-more">
            View More
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
            </svg>
        </a>
    </div>

    <style>
        .news-meta-author-page {
            font-family: "Roboto";
            font-weight: 400;
            font-size: 12px;
            color: #8c8c8c;
            letter-spacing: 0px;
            line-height: 33px;
        }


        .buying-guide-news-item {
            display: flex;
            padding: 6px 0;
            border-bottom: 1px solid #e0e0e0;
            gap: 15px;
        }

        .news-info {
            color: #262626;
            font-size: 14px;
            font-weight: 700;
            font-family: "Roboto";
            line-height: 20px;
            overflow: hidden;
            text-overflow: ellipsis;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
        }

        .view-more {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 5px;
            margin: 24px auto 0;
            padding: 8px 16px;
            border: none;
            background: none;
            color: #576B95;
            font-size: 16px;
            font-weight: 700;
            cursor: pointer;
        }

        .view-more svg {
            width: 18px;
            height: 20px;
        }
    </style>
<?php
    wp_reset_postdata();
    return ob_get_clean();
}
add_shortcode('motor_latest_news_shortcode', 'motor_latest_news_shortcode');



