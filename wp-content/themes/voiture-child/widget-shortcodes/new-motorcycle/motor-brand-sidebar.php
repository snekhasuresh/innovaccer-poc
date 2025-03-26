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
        return 'No motorcycle brands found.';
    }

    $url = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? "https" : "http") . "://$_SERVER[HTTP_HOST]$_SERVER[REQUEST_URI]";

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
    $is_new_cars_page = strpos($url, 'xe-may-moi') !== false;
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

    $filter_mapping = get_motor_filter_mapping();
    $selected_filters = parse_motor_filters_from_url($last_part, $filter_mapping);

    if (empty($selected_filters)) {
        // redirect to /cars if no filters are selected
        wp_redirect('/xe-may');
        exit;
    }

    if ($selected_filters) {
        echo do_shortcode('[motorcycle_search_filter]');
        apply_filters_to_motorcycles($selected_filters);
    } else {
        echo '<div class="widget-container" style="margin-bottom: 20px;">';
        echo do_shortcode('[motorcycle_search_filter]');
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

    // if the last part has tot-nhat- and -xe-may-tai-vietnam, remove them
    if (strpos($last_part, 'tot-nhat-') !== false && strpos($last_part, '-xe-may-tai-vietnam') !== false) {
        $last_part = str_replace(['tot-nhat-', '-xe-may-tai-vietnam'], '', $last_part);
        $parts = strpos($last_part, '-va-') === false ? [$last_part] : explode('-va-', $last_part);
    } else {
        // remove tot-nhat-va- from the last part
        $last_part = str_replace('tot-nhat-va-', '', $last_part);
        $parts = strpos($last_part, '-va-') === false ? [$last_part] : explode('-va-', $last_part);
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
            echo '<h2>No motorcycles found for the brand with ID: ' . esc_html($brand_slug) . '</h2>';
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

        echo '<div class="widget-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[motorcycle_search_filter]');
        echo '</div>';

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

        echo '<h1 class="wa-title-text find-new-cars-overview-title">Popular New Motorcycles in Philippines</h1>';
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
        echo do_shortcode('[breadcrumb]');
        echo '</div>';

        echo '<div class="widget-container populor-car-brand-container" style="margin-bottom: 20px; margin-top: 20px">';
        echo do_shortcode('[bike_brand_description brand_id="' . $brand->term_id . '"]');
        echo '</div>';

        echo '<h2 class="wa-title-text find-car-news-title">' . $brand->name . ' Motorcycles in the Philippines</h2>';

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
    $filter_mapping = [
        // Price
        'giua-vnd-0-5-tr' => ['filter-type' => 'Price', 'filter-value' => '0-5'],
        'giua-vnd-5-10-tr' => ['filter-type' => 'Price', 'filter-value' => '5-10'],
        'giua-vnd-10-20-tr' => ['filter-type' => 'Price', 'filter-value' => '10-20'],
        'giua-vnd-20-50-tr' => ['filter-type' => 'Price', 'filter-value' => '20-50'],
        'giua-vnd-50-100-tr' => ['filter-type' => 'Price', 'filter-value' => '50-100'],

        // Adventure Touring, Cafe Racer, Cruiser, Dual Sport, Moped, Off Road, Scooter, Sport, Street, Super Sport, Touring, Touring Sport
        'adventure-touring' => ['filter-type' => 'Category', 'filter-value' => 'adventure-touring'],
        'cafe-racer' => ['filter-type' => 'Category', 'filter-value' => 'cafe-racer'],
        'cruiser' => ['filter-type' => 'Category', 'filter-value' => 'cruiser'],
        'dual-sport' => ['filter-type' => 'Category', 'filter-value' => 'dual-sport'],
        'moped' => ['filter-type' => 'Category', 'filter-value' => 'moped'],
        'off-road' => ['filter-type' => 'Category', 'filter-value' => 'off-road'],
        'scooter' => ['filter-type' => 'Category', 'filter-value' => 'scooter'],
        'sport' => ['filter-type' => 'Category', 'filter-value' => 'sport'],
        'street' => ['filter-type' => 'Category', 'filter-value' => 'street'],
        'super-sport' => ['filter-type' => 'Category', 'filter-value' => 'super-sport'],
        'touring' => ['filter-type' => 'Category', 'filter-value' => 'touring'],
        'touring-sport' => ['filter-type' => 'Category', 'filter-value' => 'touring-sport'],

        // MT, AMT, CVT, DCT, AT, MCT, EV, E-CVT
        'mt' => ['filter-type' => 'Transmission', 'filter-value' => 'mt'],
        'amt' => ['filter-type' => 'Transmission', 'filter-value' => 'amt'],
        'cvt' => ['filter-type' => 'Transmission', 'filter-value' => 'cvt'],
        'dct' => ['filter-type' => 'Transmission', 'filter-value' => 'dct'],
        'at' => ['filter-type' => 'Transmission', 'filter-value' => 'at'],
        'mct' => ['filter-type' => 'Transmission', 'filter-value' => 'mct'],
        'ev' => ['filter-type' => 'Transmission', 'filter-value' => 'ev'],
        'ecvt' => ['filter-type' => 'Transmission', 'filter-value' => 'e-cvt'],

        // Petrol, Electric
        'petrol' => ['filter-type' => 'Fuel', 'filter-value' => 'petrol'],
        'ev' => ['filter-type' => 'Fuel', 'filter-value' => 'ev'],


        // 1 Seater, 2 Seater,
        '1' => ['filter-type' => 'Seating Capacity', 'filter-value' => '1 Seater'],
        '2' => ['filter-type' => 'Seating Capacity', 'filter-value' => '2 Seater'],

        // 0-150cc, 150-200cc, 200-300cc, 300-400cc, 400-500cc, 500-1000cc, Above 1000cc
        '0-150cc' => ['filter-type' => 'Displacement', 'filter-value' => '0-150cc'],
        '150-200cc' => ['filter-type' => 'Displacement', 'filter-value' => '150-200cc'],
        '200-300cc' => ['filter-type' => 'Displacement', 'filter-value' => '200-300cc'],
        '300-400cc' => ['filter-type' => 'Displacement', 'filter-value' => '300-400cc'],
        '400-500cc' => ['filter-type' => 'Displacement', 'filter-value' => '400-500cc'],
        '500-1000cc' => ['filter-type' => 'Displacement', 'filter-value' => '500-1000cc'],
        'above-1000cc' => ['filter-type' => 'Displacement', 'filter-value' => 'above-1000cc'],
    ];

    return $filter_mapping;
}
