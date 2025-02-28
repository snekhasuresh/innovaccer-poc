<?php

add_shortcode('breadcrumb', 'breadcrumb_shortcode');
function breadcrumb_shortcode($atts)
{
    $atts = shortcode_atts(['iswhite' => 'no'], $atts);

    $breadcrumb_arr = get_breadcrumb();

    if ($atts['iswhite'] == 'yes') {
        $breadcrumb_class = 'breadcrumb-white';
    } else {
        $breadcrumb_class = 'breadcrumb-default';
    }

    $breadcrumb = '<div id="breadcrumbs" class="' . esc_attr($breadcrumb_class) . '">';
    foreach ($breadcrumb_arr as $key => $value) {
        if ($key != array_key_last($breadcrumb_arr)) {
            $breadcrumb .= '<a href="' . $value . '">' . esc_html($key) . '</a>';
            $breadcrumb .= ' &gt; ';
        } else {
            $breadcrumb .= '<span class="breadcrumb-current">' . esc_html($key) . '</span>';
        }
    }
    $breadcrumb .= '</div>';

    echo $breadcrumb;
?>
    <style>
        .breadcrumb-current {
            color: #777;
        }

        .breadcrumb-white a {
            color: #fff !important;
        }
    </style>
<?php
}


function get_breadcrumb()
{
    $url_path = $_SERVER['REQUEST_URI'];
    $make = get_query_var('make');
    $model = get_query_var('model');
    $section = get_query_var('section');
    $variant_section = get_query_var('variant_section');
    $news_slug = get_query_var('news_slug');
    $breadcrumb_arr = ['Home' => '/'];

    // homepage
    if ($url_path == '/' || $url_path == '') {
        $breadcrumb_arr = ['Home' => '/'];

        // search
        if (isset($_GET['s']) && !empty($_GET['s'])) {
            $breadcrumb_arr = ['Home' => '/', 'Search Results' => ''];
        }
    }

    // search
    if (isset($_GET['s']) && !empty($_GET['s'])) {
        $breadcrumb_arr = ['Home' => '/', 'Search Results' => ''];
    }

    // cars
    if ($url_path == '/cars') {
        $breadcrumb_arr = ['Home' => '/', 'Cars' => '/cars/'];
    }

    // if make && not model
    if ($make && !$model) {
        $make = ucfirst($make);
        $breadcrumb_arr = ['Home' => '/', 'Cars' => '/cars/', $make => $url_path];
    }

    if (str_contains($url_path, 'new-cars')) {
        $breadcrumb_arr = ['Home' => '/', 'Cars' => '/cars/',  'filtered cars' => $url_path];
    }

    // individual listing
    if ($make && $model) {
        switch ($section) {
            case 'news':
                $breadcrumb_arr = [
                    'Home' => '/',
                    'Cars' => '/cars/',
                    ucfirst($make) => '/cars/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/cars/' . $make . '/' . $model . '/',
                    'news' => $url_path
                ];
                break;
            case 'specs':
                $breadcrumb_arr = [
                    'Home' => '/',
                    'Cars' => '/cars/',
                    ucfirst($make) => '/cars/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/cars/' . $make . '/' . $model,
                    'Specs' => $url_path
                ];
                break;
            case 'gallery':
                $breadcrumb_arr = [
                    'Home' => '/',
                    'Cars' => '/cars/',
                    ucfirst($make) => '/cars/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/cars/' . $make . '/' . $model,
                    'Gallery' => $url_path
                ];
                break;
            case 'fuel-consumption':
                $breadcrumb_arr = [
                    'Home' => '/',
                    'Cars' => '/cars/',
                    ucfirst($make) => '/cars/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/cars/' . $make . '/' . $model,
                    'Fuel Consumption' => $url_path
                ];
                break;
            case 'colors':
                $breadcrumb_arr = [
                    'Home' => '/',
                    'Cars' => '/cars/',
                    ucfirst($make) => '/cars/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/cars/' . $make . '/' . $model,
                    'Colors' => $url_path
                ];
                break;
            default:
                $breadcrumb_arr = [
                    'Home' => '/',
                    'Cars' => '/cars/',
                    ucfirst($make) => '/cars/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/cars/' . $make . '/' . $model
                ];
                break;
        }
    }

    // individual variant
    if ($make && $model && $variant_section) {
        $breadcrumb_arr = [
            'Home' => '/',
            'Cars' => '/cars/',
            ucfirst($make) => '/cars/' . $make . '/',
            ucfirst($make) . ' ' . ucfirst($model) => '/cars/' . $make . '/' . $model . '/',
            ucfirst($variant_section) => $url_path
        ];
    }

    // news
    if ($url_path == '/news') {
        $breadcrumb_arr = ['Home' => '/', 'News' => '/news/'];
    }

    if ($news_slug) {
        $categories = ['latest', 'reviews', 'opinions', 'evs', 'buying-guides', 'owner-stories', 'used-car', 'good-reads', 'car-tips', 'culture'];
        $breadcrumb_arr = ['Home' => '/', 'News' => '/news/'];

        // if news slug not in categories
        if (!in_array($news_slug, $categories)) {
            $news_id = get_last_numeric_id_from_url();
            if ($news_id != NULL) {
                global $wpdb;
                $result = $wpdb->get_var(
                    $wpdb->prepare("SELECT news_post_id FROM news_temp WHERE news_id = %d", $news_id)
                );
            }

            $result = $result ? $result : $news_id;
            $news_post_title = get_the_title($result);
            if (empty($news_post_title)) return;

            $breadcrumb = $news_post_title;
            $breadcrumb_arr = ['Home' => '/', 'News' => '/news/', $breadcrumb => '/news/' . $news_post_title . '/'];
        }
    }

    // cars-electric
    if ($url_path == '/cars-electric') {
        $breadcrumb_arr = ['Home' => '/', 'Cheapest Electric Cars Malaysia, Battery Electric Vehicle Brands' => '/cars-electric/'];
    }

    /***************** Tools Pages ****************/
    if ($url_path == '/tools/loan-calculator') {
        $breadcrumb_arr = ['Home' => '/', 'Tools' => '/tools/', 'Loan Calculator' => '/tools/loan-calculator/'];
    }

    if ($url_path == '/tools/road-tax-calculator') {
        $breadcrumb_arr = ['Home' => '/', 'Tools' => '/tools/', 'Road Tax Calculator' => '/tools/road-tax-calculator/'];
    }

    if ($url_path == '/tools/insurance-calculator') {
        $breadcrumb_arr = ['Home' => '/', 'Tools' => '/tools/', 'Insurance Calculator' => '/tools/insurance-calculator/'];
    }

    if ($url_path == '/tools/fuel-cost-calculator') {
        $breadcrumb_arr = ['Home' => '/', 'Tools' => '/tools/', 'Fuel Cost Calculator' => '/tools/fuel-cost-calculator/'];
    }

    if (strpos($url_path, '/compare-cars') === 0) {
        $breadcrumb_arr = ['Home' => '/', 'Compare Cars' => '/compare-cars/'];
    }

    if ($url_path == '/fuel-price') {
        $breadcrumb_arr = ['Home' => '/', 'Fuel Price' => '/fuel-price/'];
    }

    // user menu
    if ($url_path == '/me/history/') {
        $breadcrumb_arr = ['Home' => '/', 'History' => '/me/history/'];
    } else if ($url_path == '/me/favourite/') {
        $breadcrumb_arr = ['Home' => '/', 'Favorite' => '/me/favourite/'];
    } else if ($url_path == '/car-owner-service') {
        $breadcrumb_arr = ['Home' => '/', 'My Car' => '/car-owner-service/'];
    }

    // used-car-market-value-guide, trade-in-your-car
    if ($url_path == '/used-car-market-value-guide') {
        $breadcrumb_arr = ['Home' => '/', 'Used Car Market Value Guide' => '/used-car-market-value-guide/'];
    }

    if ($url_path == '/trade-in-your-car') {
        $breadcrumb_arr = ['Home' => '/', 'Trade-in Your Car' => '/trade-in-your-car/'];
    }

    // other languages
    if ($url_path == '/bm') {
        $breadcrumb_arr = ['Utama' => '/', 'Berita' => '/bm'];
    }

    if ($url_path == '/zh') {
        $breadcrumb_arr = ['首页' => '/', '新闻' => '/bm'];
    }

    return $breadcrumb_arr;
}
