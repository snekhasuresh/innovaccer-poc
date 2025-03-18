<?php

require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/findnew-search-filter.php';

function enqueue_motor_brand_sidebar_css()
{
    // Register and enqueue the CSS file
    wp_enqueue_style('brand-sidebar-style', get_stylesheet_directory_uri() . '/widget-shortcodes/new-cars/css/brand-sidebar.css', array(), '1.0', 'all');
}

add_shortcode('display_bike_brands', 'display_bike_brands_alphabetically');

function display_bike_brands_alphabetically()
{
    enqueue_motor_brand_sidebar_css();

    $brands_by_letter = get_motor_brand_sidebar_data();

    if (empty($brands_by_letter)) {
        return 'No car brands found.';
    }

    $url = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? "https" : "http") . "://$_SERVER[HTTP_HOST]$_SERVER[REQUEST_URI]";
    $is_new_cars_page = strpos($url, 'new-cars') !== false;

    ob_start();
?>
    <div id="loader" style="display: none;">
        <div class="spinner"></div>
    </div>
    <div class="car-brands-container">
        <div class="filter-container">
            <div class="alphabet-filter">
                <?php foreach (range('A', 'Z') as $letter): ?>
                    <a href="#<?= $letter ?>"><?= $letter ?></a>
                <?php endforeach; ?>
            </div>
            <div class="car-brands-sidebar">
                <div class="car-brands-list">
                    <?php
                    foreach (range('A', 'Z') as $letter) {
                        if (isset($brands_by_letter[$letter])) {
                            echo '<span class="alpha_first_letter" id="' . $letter . '">' . $letter . '</span>';

                            foreach ($brands_by_letter[$letter] as $brand) {
                                $brand_logo = $brand->logo ?: 'https://via.placeholder.com/100';
                    ?>
                                <div class="brand-item">
                                    <a href="/motorcycles/<?= esc_attr($brand->slug) ?>" class="brand-link brand-link-find-new-sidebar" data-brand-id="<?= $brand->term_id ?>">
                                        <img src="<?= esc_url($brand_logo) ?>" alt="<?= esc_attr($brand->name) ?> logo">
                                        <span><?= esc_html($brand->name) ?></span>
                                    </a>
                                </div>
                    <?php
                            }
                        }
                    }
                    ?>
                </div>
            </div>
        </div>
    </div>
<?php
    add_scripts_for_motor_brands();
    return ob_get_clean();
}

function motor_brand_details_shortcode($atts)
{
    ob_start();
    $url = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? "https" : "http") . "://$_SERVER[HTTP_HOST]$_SERVER[REQUEST_URI]";
    $is_new_cars_page = strpos($url, 'new-cars') !== false;


?>
    <div id="car-brand-details" class="car-brand-details">
        <?php
        if ($is_new_cars_page) {
            render_new_motor_details($url);
        } else {
            render_default_motor_details();
        }
        ?>
    </div>
<?php

    return ob_get_clean();
}
add_shortcode('motor_brand_details', 'motor_brand_details_shortcode');

function render_new_motor_details($url)
{
    $current_url = rtrim(parse_url($url, PHP_URL_PATH), '/');
    $path_parts = explode('/', $current_url);
    $last_part = end($path_parts);

    $filter_mapping = get_filter_mapping();
    $selected_filters = parse_motor_filters_from_url($last_part, $filter_mapping);

    if (empty($selected_filters)) {
        // redirect to /cars if no filters are selected
        wp_redirect('/cars');
        exit;
    }

    if ($selected_filters) {
        echo do_shortcode('[car_search_filter]');
        apply_filters_to_cars($selected_filters);
    } else {
        echo '<div class="widget-container" style="margin-bottom: 20px;">';
        echo do_shortcode('[car_search_filter]');
        echo '</div>';
    }
}

function parse_motor_filters_from_url($url, $filter_mapping)
{
    // Extract the path from the URL
    $parsed_url = parse_url($url);
    $path = isset($parsed_url['path']) ? trim($parsed_url['path'], '/') : '';

    // Split the path into parts
    $path_parts = explode('/', $path);
    $last_part = end($path_parts);

    // if the last part has best- and -cars-in-malaysia, remove them
    if (strpos($last_part, 'best-') !== false && strpos($last_part, '-cars-in-malaysia') !== false) {
        $last_part = str_replace(['best-', '-cars-in-malaysia'], '', $last_part);
        $parts = strpos($last_part, '-and-') === false ? [$last_part] : explode('-and-', $last_part);
    } else {
        // remove best-and- from the last part
        $last_part = str_replace('best-and-', '', $last_part);
        $parts = strpos($last_part, '-and-') === false ? [$last_part] : explode('-and-', $last_part);
    }

    $selected_filters = [];

    foreach ($parts as $part) {
        if (isset($filter_mapping[$part])) {
            $filter_category = $filter_mapping[$part]['filter-type'];
            $filter_value = $filter_mapping[$part]['filter-value'];

            // Group filters by their category
            $selected_filters[$filter_category][] = $filter_value;
        }
    }

    return $selected_filters;
}

function render_default_motor_details()
{
    $brand_slug = get_query_var('make');
    if ($brand_slug) {
        $brand = get_term_by('slug', $brand_slug, 'motorcycle_make');
        if ($brand && !is_wp_error($brand)) {
            apply_bike_brand_filter($brand);
        } else {
            echo '<h2>No cars found for the brand with ID: ' . esc_html($brand_slug) . '</h2>';
        }
    } else {
        show_all_motors();
    }
}

function add_scripts_for_motor_brands()
{
?>
    <!-- Script to Scroll to Car Details Section on Brand Click -->
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            if (window.matchMedia("(max-width: 768px)").matches) {
                const detailsSection = document.getElementById('car-brand-details');
                if (detailsSection) {
                    detailsSection.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });
                }

                const brandLinks = document.querySelectorAll('.brand-link');
                brandLinks.forEach(link => {
                    link.addEventListener('click', function(event) {
                        event.preventDefault();

                        const targetHref = link.getAttribute('href');
                        if (targetHref) {
                            history.pushState(null, '', targetHref);
                            window.location.href = targetHref;
                        }
                    });
                });
            }
        });
    </script>
    <script>
        document.addEventListener('DOMContentLoaded', () => {
            const brandList = document.querySelector('.car-brands-list');
            const alphabetLinks = document.querySelectorAll('.alphabet-filter a');

            brandList.addEventListener('scroll', () => {
                let activeLetter = null;
                document.querySelectorAll('.alpha_first_letter').forEach(section => {
                    if (brandList.scrollTop >= section.offsetTop - 50) {
                        activeLetter = section.id;
                    }
                });

                alphabetLinks.forEach(link => {
                    link.classList.toggle('active', link.textContent === activeLetter);
                });
            });

            // Default active letter
            alphabetLinks[0]?.classList.add('active');
        });
    </script>
<?php
}

if (!function_exists('show_all_motors')) {
    function show_all_motors()
    {
           echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px">';
			echo do_shortcode('[breadcrumb]');
			echo '</div>';

        // echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px">';
        // echo do_shortcode('[car_search_filter]');
        // echo '</div>';

        // echo '<h2>Popular Cars in Malaysia</h2>';
        echo '<div class="widget-container populor-car-brand-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[popular_bike_in_new_bikes]');
        echo '</div>';


        echo '<div class="widget-container populor-car-brand-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[popular_bike_brands]');
        echo '</div>';

        // echo '<h2>Latest Car Videos</h2>';
        echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px; ">';
        echo do_shortcode('[findnew_motor_videos_carousal]');
        echo '</div>';

        // echo '<h2>Latest Car News</h2>';
        echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px;">';
        echo do_shortcode('[motor_related_news]');
        echo '</div>';

        // echo '<h2 class="wa-title-text">เปรียบเทียบรถมอเตอร์ไซค์ </h2>';
        // echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px; margin-left:-22px">';
        // echo do_shortcode('[findnew_bike_comparison]');
        // echo '</div>';

        // echo '<h2 class="wa-title-text">Upcoming Cars</h2>';
        // echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px;  margin-left:-33px">';
        // echo do_shortcode('[upcoming_bikes]');
        // echo '</div>';

//         echo '<h2 class="wa-title-text">คำถามที่พบบ่อยเกี่ยวกับรถมอเตอร์ไซค์</h2>';
        echo '<div class="widget-container populor-car-brand-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[findnew_bike_faqs_shortcode]');
        echo '</div>';

        echo '<h1 class="wa-title-text find-new-cars-overview-title">Xe máy mới phổ biến tại Việt Nam</h1>';
        echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[newbike_overview]');
        echo '</div>';
    }
}

if (!function_exists('apply_bike_brand_filter')) {
    function apply_bike_brand_filter($brand)
    {
        ob_start();
        echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[ev_top_banner_for_newcars]');
        echo '</div>';

        echo '<div class="widget-container populor-car-brand-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[bike_brand_description brand_id="' . $brand->term_id . '"]');
        echo '</div>';

        echo '<h2 class="wa-title-text find-car-news-title">รถมอเตอร์ไซค์ ' . $brand->name . ' ในไทย</h2>';

        echo '<div class="widget-container  populor-car-brand-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[popular_bike_in_new_bikes brand_id="' . $brand->term_id . '"]');
        echo '</div>';

        // echo '<h2>' . $brand->name . ' News in Malaysia</h2>';
        echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[motor_related_news brand_id="' . $brand->term_id . ' brand_name="' . $brand->name . '"]');
        echo '</div>';

        // echo '<h2>' . $brand->name . ' Videos in Malaysia</h2>';
        echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[findnew_motor_videos_carousal brand_id="' . $brand->term_id . ' brand_name="' . $brand->name . '"]');
        echo '</div>';

//         echo '<h2 class="wa-title-text"> เปรียบเทียบรถมอเตอร์ไซค์ ' . $brand->name. '</h2>';
        echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[findnew_bike_comparison brand_id="' . $brand->term_id . '"]');
        echo '</div>';

        // echo '<h2>Recommended Cars</h2>';
        // echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px">';
        // echo do_shortcode('[find_new_recommended_cars brand_id="' . $brand->term_id . '"]');
        // echo '</div>';

//         echo '<h2 class="wa-title-text"> คำถามที่พบบ่อยเกี่ยวกับรถมอเตอร์ไซค์ ' . $brand->name . ' 2024</h2>';
        echo '<div class="widget-container filtered-car-comparison-con" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[findnew_bike_faqs_shortcode brand_id="' . $brand->term_id . '"]');
        echo '</div>';

        // echo '<h2>Popular Car Brands in Malaysia</h2>';
        echo '<div class="widget-container populor-car-brand-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[popular_bike_brands]');
        echo '</div>';
        $output = ob_get_clean();

        echo $output;
    }
}


function get_motor_filter_mapping()
{
    // read url
    $url = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? "https" : "http") . "://$_SERVER[HTTP_HOST]$_SERVER[REQUEST_URI]";
    // if url has new-cars in it like http://34.126.131.237/new-cars/best-suv-sedan/
    if (strpos($url, 'new-cars') == true) {
        $body_type_terms = get_terms(array('taxonomy' => 'listing_type', 'hide_empty' => false));
        $filter_mapping = [];

        $filter_mapping = [
            // 0-40K, 40-60K, 60-90K, 90-120K, 120-150K, 150-200K, 200-300K, 300-400K, 400-600K, 600-3000K
            'between-rm-0-40k' => ['filter-type' => 'Price', 'filter-value' => '0-40'],
            'between-rm-40-60k' => ['filter-type' => 'Price', 'filter-value' => '40-60'],
            'between-rm-60-90k' => ['filter-type' => 'Price', 'filter-value' => '60-90'],
            'between-rm-90-120k' => ['filter-type' => 'Price', 'filter-value' => '90-120'],
            'between-rm-120-1500k' => ['filter-type' => 'Price', 'filter-value' => '120-150'],
            'between-rm-150-200k' => ['filter-type' => 'Price', 'filter-value' => '150-200'],
            'between-rm-200-300k' => ['filter-type' => 'Price', 'filter-value' => '200-300'],
            'between-rm-300-400k' => ['filter-type' => 'Price', 'filter-value' => '300-400'],
            'between-rm-400-6000k' => ['filter-type' => 'Price', 'filter-value' => '400-600'],
            'between-rm-600-3000k' => ['filter-type' => 'Price', 'filter-value' => '600-3000'],

            // A-Segment, B-Segment, C-Segment, D-Segment, E-Segment, Commercial, Executive, Grand Tourer, Luxury, Sports Car, Super Car, Compact Executive, 4x4, 4x2
            'a-segment' => ['filter-type' => 'Segment', 'filter-value' => 'A-Segment'],
            'b-segment' => ['filter-type' => 'Segment', 'filter-value' => 'B-Segment'],
            'c-segment' => ['filter-type' => 'Segment', 'filter-value' => 'C-Segment'],
            'd-segment' => ['filter-type' => 'Segment', 'filter-value' => 'D-Segment'],
            'e-segment' => ['filter-type' => 'Segment', 'filter-value' => 'E-Segment'],
            'commercial' => ['filter-type' => 'Segment', 'filter-value' => 'Commercial'],
            'executive' => ['filter-type' => 'Segment', 'filter-value' => 'Executive'],
            'grand-tourer' => ['filter-type' => 'Segment', 'filter-value' => 'Grand Tourer'],
            'luxury' => ['filter-type' => 'Segment', 'filter-value' => 'Luxury'],
            'sports-car' => ['filter-type' => 'Segment', 'filter-value' => 'Sports Car'],
            'super-car' => ['filter-type' => 'Segment', 'filter-value' => 'Super Car'],
            'compact-executive' => ['filter-type' => 'Segment', 'filter-value' => 'Compact Executive'],
            '4x4' => ['filter-type' => 'Segment', 'filter-value' => '4x4'],
            '4x2' => ['filter-type' => 'Segment', 'filter-value' => '4x2'],

            // MT, AMT, CVT, DCT, AT, MCT, EV, E-CVT
            'mt' => ['filter-type' => 'Transmission', 'filter-value' => 'mt'],
            'amt' => ['filter-type' => 'Transmission', 'filter-value' => 'amt'],
            'cvt' => ['filter-type' => 'Transmission', 'filter-value' => 'cvt'],
            'dct' => ['filter-type' => 'Transmission', 'filter-value' => 'dct'],
            'at' => ['filter-type' => 'Transmission', 'filter-value' => 'at'],
            'mct' => ['filter-type' => 'Transmission', 'filter-value' => 'mct'],
            'ev' => ['filter-type' => 'Transmission', 'filter-value' => 'ev'],
            'ecvt' => ['filter-type' => 'Transmission', 'filter-value' => 'e-cvt'],

            // Petrol, Diesel, Petrol Hybrid, Diesel Hybrid, Electric Vehicle
            'petrol' => ['filter-type' => 'Fuel', 'filter-value' => 'petrol'],
            'diesel' => ['filter-type' => 'Fuel', 'filter-value' => 'diesel'],
            'petrol-hybrid' => ['filter-type' => 'Fuel', 'filter-value' => 'petrol-hybrid'],
            'diesel-hybrid' => ['filter-type' => 'Fuel', 'filter-value' => 'diesel-hybrid'],
            'electric-vehicle' => ['filter-type' => 'Fuel', 'filter-value' => 'electric-vehicle'],

            // 2 Seater, 4 Seater, 5 Seater, 6 Seater, 7 Seater, 8 Seater, 9 Seater
            '2' => ['filter-type' => 'Seating Capacity', 'filter-value' => '2 Seater'],
            '4' => ['filter-type' => 'Seating Capacity', 'filter-value' => '4 Seater'],
            '5' => ['filter-type' => 'Seating Capacity', 'filter-value' => '5 Seater'],
            '6' => ['filter-type' => 'Seating Capacity', 'filter-value' => '6 Seater'],
            '7' => ['filter-type' => 'Seating Capacity', 'filter-value' => '7 Seater'],
            '8' => ['filter-type' => 'Seating Capacity', 'filter-value' => '8 Seater'],
            '9' => ['filter-type' => 'Seating Capacity', 'filter-value' => '9 Seater'],

            // Front Wheel Drive, Rear Wheel Drive, All Wheel Drive
            'forwardwheeldrive' => ['filter-type' => 'Drive Type', 'filter-value' => 'fwd'],
            'rearwheeldrive' => ['filter-type' => 'Drive Type', 'filter-value' => 'rwd'],
            'allwheeldrive' => ['filter-type' => 'Drive Type', 'filter-value' => 'awd'],
        ];

        foreach ($body_type_terms as $term) {
            $term_name = strtolower($term->name);
            $filter_mapping[$term_name] = ['filter-type' => 'Body Type', 'filter-value' => $term->term_id];
        }
    }

    return $filter_mapping;
}
