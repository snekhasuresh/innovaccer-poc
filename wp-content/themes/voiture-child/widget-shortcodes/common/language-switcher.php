<?php
add_shortcode('custom_language_switcher', 'custom_language_switcher_shortcode');
function custom_language_switcher_shortcode($atts)
{
    // Enqueue the CSS file
    wp_enqueue_style('language-switcher-style', get_stylesheet_directory_uri() . '/widget-shortcodes/common/css/language-switcher.css');
    // Enqueue the JavaScript file with version parameter to prevent caching
    wp_enqueue_script(
        'language-switcher-script',
        get_stylesheet_directory_uri() . '/widget-shortcodes/common/js/language-switcher.js',
        array(),
        time(), // Add timestamp as version to prevent caching
        true
    );

    $atts = shortcode_atts(array('page' => 'home'), $atts);

    $currentUrl = home_url();
    $url_parts = explode('/', $currentUrl);
    $baseUrl = $url_parts[0] . '/' . $url_parts[2];

    if (isset($_GET['lang'])) {
        $baseUrl .= '/' . $_GET['lang'];
    }

    $base_url = home_url();
    $languages = [
        'home' => [
            'English' => home_url(''),
            'Bahasa Malaysia' => home_url(''),
            '中文' => home_url('') //Chinese
        ],
        'news' => [
            'English' => $base_url . '/news/latest',
            'Bahasa Malaysia' => $base_url . '/bm',
            '中文' => $base_url . '/zh'
        ]
    ];

    // Get the current language from cookie or default to English
    $current_language = isset($_COOKIE['preferred_language']) ? $_COOKIE['preferred_language'] : 'English';

    ob_start();
?>
    <!-- Added comment for debugging -->
    <!-- Language Switcher Start -->
    <?php if (isset($languages[$atts['page']])) : ?>
        <select id="language-switcher">
            <?php foreach ($languages[$atts['page']] as $label => $url) : ?>
                <option value="<?php echo esc_url($url); ?>" <?php selected($current_language, $label); ?>>
                    <?php echo esc_html($label); ?>
                </option>
            <?php endforeach; ?>
        </select>
    <?php endif; ?>
    <!-- Language Switcher End -->
<?php
    return ob_get_clean();
}
