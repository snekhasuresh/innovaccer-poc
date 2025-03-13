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
    $breadcrumb_arr = ['Trang chủ' => '/'];
	$is_motorcycle = strpos($_SERVER['REQUEST_URI'], '/xe-may/') !== false;
    $is_car = strpos($_SERVER['REQUEST_URI'], '/xe-oto/') !== false;

    // homepage
    if ($url_path == '/' || $url_path == '') {
        $breadcrumb_arr = ['Trang chủ' => '/'];

        // search
        if (isset($_GET['s']) && !empty($_GET['s'])) {
            $breadcrumb_arr = ['Trang chủ' => '/', 'Search Results' => ''];
        }
    }

    // search
    if (isset($_GET['s']) && !empty($_GET['s'])) {
        $breadcrumb_arr = ['Trang chủ' => '/', 'Search Results' => ''];
    }

    // cars
    if ($url_path == '/xe-oto') {
        $breadcrumb_arr = ['Trang chủ' => '/', 'Xe ô tô' => '/xe-oto/'];
    }
	
	//motorcycle
	 if ($url_path == '/xe-may') {
        $breadcrumb_arr = ['Trang chủ' => '/', 'Xe Máy' => '/xe-may/'];
    }

    // if make && not model
    if($is_car){
    if ($make && !$model) {
        $make = ucfirst($make);
        $breadcrumb_arr = ['Trang chủ' => '/', 'Xe ô tô' => '/xe-oto/', $make => $url_path];
    }
	}
	
	 if($is_motorcycle){
    if ($make && !$model) {
        $make = ucfirst($make);
        $breadcrumb_arr = ['Trang chủ' => '/', 'Xe Máy' => '/xe-may/', $make => $url_path];
    }
	}

    if (str_contains($url_path, 'xe-hoi-moi')) {
        $breadcrumb_arr = ['Trang chủ' => '/', 'Xe ô tô' => '/xe-oto/',  'filtered cars' => $url_path];
    }

    // individual listing
    if($is_car){
    if ($make && $model) {
        switch ($section) {
            case 'tin-tuc':
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe ô tô' => '/xe-oto/',
                    ucfirst($make) => '/xe-oto/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-oto/' . $make . '/' . $model . '/',
                    'Tin tức '. ucfirst($make) . ' ' . ucfirst($model) => $url_path
                ];
                break;
            case 'thong-so-ky-thuat':
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe ô tô' => '/xe-oto/',
                    ucfirst($make) => '/xe-oto/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-oto/' . $make . '/' . $model,
                    'Thông số '. ucfirst($make) . ' ' . ucfirst($model) => $url_path
                ];
                break;
            case 'hinh-anh':
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe ô tô' => '/xe-oto/',
                    ucfirst($make) => '/xe-oto/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-oto/' . $make . '/' . $model,
                    'Hình ảnh' => $url_path
                ];
                break;
            case 'tieu-hao-nhien-lieu':
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe ô tô' => '/xe-oto/',
                    ucfirst($make) => '/xe-oto/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-oto/' . $make . '/' . $model,
                    'Mức tiêu hao nhiên liệu của'.ucfirst($make) . ' ' . ucfirst($model) => $url_path
                ];
                break;
            case 'mau-sac':
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe ô tô' => '/xe-oto/',
                    ucfirst($make) => '/xe-oto/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-oto/' . $make . '/' . $model,
                    'Màu Sắc '.ucfirst($make) . ' ' . ucfirst($model) => $url_path
                ];
                break;
            default:
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe ô tô' => '/xe-oto/',
                    ucfirst($make) => '/xe-oto/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-oto/' . $make . '/' . $model
                ];
                break;
        }
    }
}
	if($is_motorcycle){
		if ($make && $model) {
        switch ($section) {
            case 'tin-tuc':
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe Máy' => '/xe-may/',
                    ucfirst($make) => '/xe-may/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-may/' . $make . '/' . $model . '/',
                    'Tin tức '.ucfirst($make) . ' ' . ucfirst($model)  => $url_path
                ];
                break;
            case 'thong-so-ky-thuat':
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe Máy' => '/xe-may/',
                    ucfirst($make) => '/xe-may/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-may/' . $make . '/' . $model,
                    'Thông số '.ucfirst($make) . ' ' . ucfirst($model) => $url_path
                ];
                break;
            case 'hinh-anh':
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe Máy' => '/xe-may/',
                    ucfirst($make) => '/xe-may/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-may/' . $make . '/' . $model,
                    'Hình ảnh' => $url_path
                ];
                break;
            case 'tieu-hao-nhien-lieu':
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe Máy' => '/xe-may/',
                    ucfirst($make) => '/xe-may/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-may/' . $make . '/' . $model,
                    'Tiêu Thụ Nhiên Liệu '.ucfirst($make) . ' ' . ucfirst($model) => $url_path
                ];
                break;
            case 'mau-sac':
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe Máy' => '/xe-may/',
                    ucfirst($make) => '/xe-may/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-may/' . $make . '/' . $model,
                    'Màu Sắc '. ucfirst($make) . ' ' . ucfirst($model) => $url_path
                ];
                break;
            default:
                $breadcrumb_arr = [
                    'Trang chủ' => '/',
                    'Xe Máy' => '/xe-may/',
                    ucfirst($make) => '/xe-may/' . $make . '/',
                    ucfirst($make) . ' ' . ucfirst($model) => '/xe-may/' . $make . '/' . $model
                ];
                break;
        }
    }
	}

    // individual variant
    if($is_car){
    if ($make && $model && $variant_section) {
        $breadcrumb_arr = [
            'Trang chủ' => '/',
            'Xe ô tô' => '/xe-oto/',
            ucfirst($make) => '/xe-oto/' . $make . '/',
            ucfirst($make) . ' ' . ucfirst($model) => '/xe-oto/' . $make . '/' . $model . '/',
            ucfirst($variant_section) => $url_path
        ];
    }
	}
	
	if($is_motorcycle){
    if ($make && $model && $variant_section) {
        $breadcrumb_arr = [
            'Trang chủ' => '/',
            'Xe Máy' => '/xe-may/',
            ucfirst($make) => '/xe-may/' . $make . '/',
            ucfirst($make) . ' ' . ucfirst($model) => '/xe-may/' . $make . '/' . $model . '/',
            ucfirst($variant_section) => $url_path
        ];
    }
	}

    // news
    if ($url_path == '/tin-tuc') {
        $breadcrumb_arr = ['Trang chủ' => '/', 'Tin tức' => '/tin-tuc/'];
    }

    if ($news_slug) {
        $categories = ['latest', 'reviews', 'opinions', 'evs', 'buying-guides', 'owner-stories', 'used-car', 'good-reads', 'car-tips', 'culture'];
        $breadcrumb_arr = ['Trang chủ' => '/', 'Tin tức' => '/tin-tuc/'];

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
            $breadcrumb_arr = ['Trang chủ' => '/', 'Tin tức' => '/tin-tuc/', $breadcrumb => '/tin-tuc/' . $news_post_title . '/'];
        }
    }

    // cars-electric
//     if ($url_path == '/cars-electric') {
//         $breadcrumb_arr = ['Home' => '/', 'Cheapest Electric Cars Malaysia, Battery Electric Vehicle Brands' => '/cars-electric/'];
//     }

    /***************** Tools Pages ****************/
    if ($url_path == '/dung-cu/mua-xe-tra-gop') {
        $breadcrumb_arr = ['Trang chủ' => '/', 'Công cụ' => '/dung-cu/', 'Mua Xe Trả Góp' => '/dung-cu/mua-xe-tra-gop/'];
    }

//     if ($url_path == '/tools/road-tax-calculator') {
//         $breadcrumb_arr = ['Home' => '/', 'Tools' => '/tools/', 'Road Tax Calculator' => '/tools/road-tax-calculator/'];
//     }

    if ($url_path == '/dung-cu/bao-hiem-xe') {
        $breadcrumb_arr = ['Trang chủ' => '/', 'Công cụ' => '/dung-cu/', 'Bảo Hiểm Xe' => '/dung-cu/bao-hiem-xe/'];
    }

//     if ($url_path == '/tools/fuel-cost-calculator') {
//         $breadcrumb_arr = ['Home' => '/', 'Tools' => '/tools/', 'Fuel Cost Calculator' => '/tools/fuel-cost-calculator/'];
//     }

    if (strpos($url_path, '/so-sanh-xe') === 0) {
        $breadcrumb_arr = ['Trang chủ' => '/', 'So Sánh Xe Ô Tô' => '/so-sanh-xe/'];
    }

    if ($url_path == '/dung-cu/gia-xang-dau') {
        $breadcrumb_arr = ['Trang chủ' => '/','Dụng Cụ' => '/dung-cu/','Giá Xăng Dầu' => '/dung-cu/gia-xang-dau/'];
    }
	
	//others
	if ($url_path == '/user-agreement') {
        $breadcrumb_arr = ['Trang chủ' => '/','Thoả thuận người dùng' => '/user-agreement/'];
    }
	
	if ($url_path == '/privacy-policy') {
        $breadcrumb_arr = ['Trang chủ' => '/','Chính sách bảo mật' => '/privacy-policy/'];
    }
	
	if ($url_path == '/about-us') {
        $breadcrumb_arr = ['Trang chủ' => '/','Về chúng tôi' => '/about-us/'];
    }
	
	if ($url_path == '/viet-cho-chung-toi') {
        $breadcrumb_arr = ['Trang chủ' => '/','Viết Đối Với Chúng Tôi' => '/viet-cho-chung-toi/'];
    }
	
	if ($url_path == '/quang-cao-voi-chung-toi') {
        $breadcrumb_arr = ['Trang chủ' => '/','Quảng cáo với chúng tôi' => '/quang-cao-voi-chung-toi/'];
    }

    // user menu
//     if ($url_path == '/me/history/') {
//         $breadcrumb_arr = ['Home' => '/', 'History' => '/me/history/'];
//     } else if ($url_path == '/me/favourite/') {
//         $breadcrumb_arr = ['Home' => '/', 'Favorite' => '/me/favourite/'];
//     } else if ($url_path == '/car-owner-service') {
//         $breadcrumb_arr = ['Home' => '/', 'My Car' => '/car-owner-service/'];
//     }

    // used-car-market-value-guide, trade-in-your-car
//     if ($url_path == '/used-car-market-value-guide') {
//         $breadcrumb_arr = ['Home' => '/', 'Used Car Market Value Guide' => '/used-car-market-value-guide/'];
//     }

//     if ($url_path == '/trade-in-your-car') {
//         $breadcrumb_arr = ['Home' => '/', 'Trade-in Your Car' => '/trade-in-your-car/'];
//     }

    // other languages
    if ($url_path == '/bm') {
        $breadcrumb_arr = ['Utama' => '/', 'Berita' => '/bm'];
    }

    if ($url_path == '/zh') {
        $breadcrumb_arr = ['首页' => '/', '新闻' => '/bm'];
    }

    return $breadcrumb_arr;
}
