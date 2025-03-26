<?php
// import search-filter-shortcode.css
function enqueue_motor_search_filter_shortcode_css()
{
    wp_enqueue_style('motorcycle-search-filter-shortcode', get_stylesheet_directory_uri() . '/widget-shortcodes/new-cars/css/search-filter-shortcode.css');
}

// import apply-filter-to-cars.css
function enqueue_apply_filter_to_motorcycles_css()
{
    wp_enqueue_style('apply-filter-to-motorcycles', get_stylesheet_directory_uri() . '/widget-shortcodes/new-cars/css/apply-filter-to-cars.css');
}

function motorcycle_search_filter_shortcode()
{
    enqueue_motor_search_filter_shortcode_css();

    // price range
    $price_ranges = [
        'all' => 'All',
        '0-5' => '0 -5 tr',
        '5-10' => '5 - 10 tr',
        '10-20' => '10 - 20 tr',
        '20-50' => '20 - 50 tr',
        '50-100' => '50 - 100 tr',
    ];

    // categories
    $categories = [
        'all' => 'All',
        'adventure-touring' => 'Adventure Touring',
        'cafe-racer' => 'Cafe Racer',
        'cruiser' => 'Cruiser',
        'dual-sport' => 'Dual Sport',
        'moped' => 'Moped',
        'off-road' => 'Off Road',
        'scooter' => 'Scooter',
        'sport' => 'Sport',
        'street' => 'Street',
        'super-sport' => 'Super Sport',
        'touring' => 'Touring',
        'touring-sport' => 'Touring Sport',
    ];

    // transmission
    $transmissions = [
        'all' => 'All',
        'mt' => 'Manual',
        'at' => 'Automatic',
        'cvt' => 'CVT',
        'dct' => 'Dual Clutch',
    ];

    // fuel
    $fuels = [
        'all' => 'All',
        'petrol' => 'Petrol',
        'ev' => 'Electric'
    ];

    // seats
    $seats = [
        'all' => 'All',
        '1' => '1 seater',
        '2' => '2 seater',
    ];

    // displacement
    $displacement = [
        'all' => 'All',
        '0-150-cc' => '0-150cc',
        '150-200-cc' => '150-200cc',
        '200-300-cc' => '200-300cc',
        '300-400-cc' => '300-400cc',
        '400-500-cc' => '400-500cc',
        '500-1000-cc' => '500-1000cc',
        'above-1000-cc' => 'Above 1000cc',
    ];

    $all_filters = [];
    $all_filters['price_range'] = array('label' => 'Price', 'values' => $price_ranges);
    $all_filters['category'] = array('label' => 'Category', 'values' => $categories);
    $all_filters['transmission'] = array('label' => 'Transmission', 'values' => $transmissions);
    $all_filters['fuel'] = array('label' => 'Fuel', 'values' => $fuels);
    $all_filters['seats'] = array('label' => 'Seats', 'values' => $seats);
    $all_filters['displacement'] = array('label' => 'Displacement', 'values' => $displacement);
    ob_start();
?>
    <div class="car-search-filters">
        <h2 class="wa-title-text">Search Filters</h2>
        <div id="car-search-filters"></div>
        <?php foreach ($all_filters as $key => $value): ?>
            <div class="filter-group">
                <label><?php echo $value['label']; ?></label>
                <ul>
                    <?php foreach ($value['values'] as $key => $label): ?>
                        <li>
                            <button class="filter-option"
                                data-filter="<?php echo $value['label']; ?>"
                                data-value="<?php echo $key; ?>">
                                <?php echo $label; ?>
                            </button>
                        </li>
                    <?php endforeach; ?>
                </ul>
            </div>
        <?php endforeach; ?>
    </div>

    <!-- <div class="more-options-container">
        <div class="line"></div>
        <button class="more-options-toggle">
            More Options <span style="margin-left: 5px;"><i class="fas fa-chevron-down"></i></span>
        </button>
        <div class="line"></div>
    </div> -->
    <div id="selected-filters-container">
    </div>

    <script type="text/javascript">
        /*document.addEventListener('DOMContentLoaded', function() {
            const moreOptionsToggle = document.querySelector('.more-options-toggle');
            const filterGroups = document.querySelectorAll('.filter-group');
            const moreOptionsText = 'More Options <span style="margin-left: 5px;"><i class="fas fa-chevron-down"></i></span>';
            const closeOptionsText = 'Close Options <span style="margin-left: 5px;"><i class="fas fa-chevron-up"></i></span>';

            // Initially hide all filters after the first five
            filterGroups.forEach((group, index) => {
                if (index >= 5) {
                    group.style.display = 'none';
                }
            });

            moreOptionsToggle.addEventListener('click', function() {
                const isExpanded = moreOptionsToggle.classList.contains('expanded');

                if (isExpanded) {
                    // Collapse: Hide filters after the first five
                    filterGroups.forEach((group, index) => {
                        if (index >= 10) {
                            group.style.display = 'none';
                        }
                    });
                    moreOptionsToggle.innerHTML = moreOptionsText;
                    moreOptionsToggle.classList.remove('expanded');
                } else {
                    // Expand: Show all filters
                    filterGroups.forEach((group) => {
                        group.style.display = 'flex';
                    });
                    moreOptionsToggle.innerHTML = closeOptionsText;
                    moreOptionsToggle.classList.add('expanded');
                }
            });
        });*/


        let selectedFilters = {};
        var slug_map = slug_map || {};
        Object.assign(slug_map, {
            // price range
            '0-5': 'giua-vnd-0-5-tr',
            '5-10': 'giua-vnd-5-10-tr',
            '10-20': 'giua-vnd-10-20-tr',
            '20-50': 'giua-vnd-20-50-tr',
            '50-100': 'giua-vnd-50-100-tr',

            // categories
            'adventure-touring': 'adventure-touring',
            'cafe-racer': 'cafe-racer',
            'cruiser': 'cruiser',
            'dual-sport': 'dual-sport',
            'moped': 'moped',
            'off-road': 'off-road',
            'scooter': 'scooter',
            'sport': 'sport',
            'street': 'street',
            'super-sport': 'super-sport',
            'touring': 'touring',
            'touring-sport': 'touring-sport',

            // transmissions
            'mt': 'mt',
            'at': 'at',
            'cvt': 'cvt',
            'dct': 'dct',

            // fuels
            'petrol': 'petrol',
            'ev': 'ev',

            // seats
            '1': '1',
            '2': '2',

            // displacement
            '0-150-cc': '0-150cc',
            '150-200-cc': '150-200cc',
            '200-300-cc': '200-300cc',
            '300-400-cc': '300-400cc',
            '400-500-cc': '400-500cc',
            '500-1000-cc': '500-1000cc',
            'above-1000-cc': 'above-1000cc',
        });

        document.addEventListener('DOMContentLoaded', function() {
            // add event listeners to filter options
            var filterOptions = document.querySelectorAll('.filter-option');

            var url = window.location.href;
            if (url.endsWith('/')) {
                url = url.slice(0, -1);
            }
            var parts = url.split('/');
            var filter = parts[parts.length - 1];

            var filter_parts = [];
            // if the last part has tot-nhat- and -xe-may-tai-vietnam, remove them
            if (filter.includes('tot-nhat-') && filter.includes('-xe-may-tai-vietnam')) {
                filter = filter.replace('tot-nhat-', '');
                filter = filter.replace('-xe-may-tai-vietnam', '');
            } else {
                filter = filter.replace('tot-nhat', '');
            }

            filter_parts = filter.includes('-va-') ? filter.split('-va-') : [filter];
            all_filters = <?php echo json_encode($all_filters); ?>;

            // check if filter_parts contains 'tot-nhat'
            var valid_url = filter_parts.length > 0;
            if (valid_url) {
                // get keys of filter_parts from slug_map
                for (var i = 0; i < filter_parts.length; i++) {
                    var value = filter_parts[i];
                    // get key of value from slug_map
                    var key = Object.keys(slug_map).find(key => slug_map[key] === value);
                    if (key) {
                        filter_parts[i] = key;
                    }
                }
            }
            // if url is baseurl/cars
            if (filter_parts.length === 1 && filter_parts[0] === 'xe-may') {
                valid_url = true;
            }

            var matchingLabels = [];
            filter_parts.forEach(part => {
                for (const key in all_filters) {
                    if (Object.keys(all_filters[key].values).includes(part)) {
                        matchingLabels.push(all_filters[key].label);
                        break; // No need to check further once a match is found for this label
                    }
                }
            });

            matchingLabels = matchingLabels.filter((label, index) => matchingLabels.indexOf(label) === index);

            filterOptions.forEach(function(option) {
                // get key of filter_parts
                // if the option's dataset.value matches the filter_parts, add the highlight class

                if (valid_url) {
                    if (filter_parts.includes(option.dataset.value)) {
                        option.classList.add('highlight-yellow');
                    }

                    // add highlight yellow class to all options initially
                    if (option.dataset.value === 'all' && !matchingLabels.includes(option.dataset.filter)) {
                        option.classList.add('highlight-yellow');

                        option.addEventListener('click', function() {
                            // if the option is already highlighted, don't remove the highlight
                            // if the option is not already highlighted, add the highlight
                            if (!this.classList.contains('highlight-yellow')) {
                                this.classList.add('highlight-yellow');
                            }
                            var filter = this.dataset.filter;
                            delete selectedFilters[filter];
                            // if (Object.keys(selectedFilters).length === 0) {
                            //     selectedFilters = {};
                            // }

                            // remove highlight from all other options for this filter
                            var options = document.querySelectorAll('.filter-option[data-filter="' + filter + '"]');
                            options.forEach(function(option) {
                                if (option.dataset.value !== 'all') {
                                    option.classList.remove('highlight-yellow');
                                }
                            });
                        })
                    }
                }


                // add event listener to filter option
                option.addEventListener('click', function() {
                    // if the option is not the all option
                    if (option.dataset.value !== 'all') {

                        // if all option for the filter is highlighted, remove the highlight
                        var filter = this.dataset.filter;
                        var filterSpecificOptions = document.querySelectorAll('.filter-option[data-filter="' + filter + '"]');
                        filterSpecificOptions.forEach(function(filterSpecificOption) {
                            if (filterSpecificOption.dataset.value === 'all') {
                                filterSpecificOption.classList.remove('highlight-yellow');
                            }
                        });

                        if (this.classList.contains('highlight-yellow')) {
                            this.classList.remove('highlight-yellow');
                            var filter = this.dataset.filter;
                            var value = this.dataset.value;

                            // if filters are not selected for this particular filter, highlight the all option
                            if (selectedFilters[filter] && selectedFilters[filter].length === 1) {
                                filterSpecificOptions.forEach(function(filterSpecificOption) {
                                    if (filterSpecificOption.dataset.value === 'all') {
                                        filterSpecificOption.classList.add('highlight-yellow');
                                    }
                                });
                            }

                            if (selectedFilters[filter]) {
                                selectedFilters[filter].push(value);
                            } else {
                                selectedFilters[filter] = [value];
                            }
                            this.classList.add('highlight-yellow');
                        }
                    }
                });

            });
        })

        jQuery(document).ready(function($) {
            $('.filter-option').on('click', function() {
                var filter = $(this).data('filter');
                var value = $(this).data('value');
                selectedFilters = {};
                if (filter == 'Body Type') {
                    value = String(value);
                }
                selectedFilters[filter] = [value];

                url = window.location.href;
                let existingFilters = <?php echo json_encode(get_existing_motorcycle_filters()); ?>;
                if (!existingFilters) {
                    existingFilters = {};
                }
                if (value === 'all') {
                    delete selectedFilters[filter];
                    delete existingFilters[filter];
                }
                var url = constructURL(selectedFilters, existingFilters);
                console.log(url);
                window.location.href = url;
            })
        });

        function constructURL($selectedFilters, existingFilters) {
            let outerUnion = getOuterUnion($selectedFilters, existingFilters);
            outerUnion = Object.fromEntries(Object.entries(outerUnion).filter(([_, v]) => v.length > 0));

            if (Object.keys(outerUnion).length === 0) {
                return "<?php echo home_url('/xe-may'); ?>";
            }

            const base_url = "<?php echo home_url('/'); ?>" + 'xe-may-moi/tot-nhat';
            let url = base_url;

            let all_valid_filters = [];
            for (const [key, value] of Object.entries(outerUnion)) {
                if (value.length > 0) {
                    for (var i = 0; i < value.length; i++) {
                        if (slug_map[value[i]] !== undefined) {
                            all_valid_filters.push(slug_map[value[i]]);
                        }
                    }
                }
            }

            if (all_valid_filters.length === 1) {
                url = url + '-' + all_valid_filters[0] + '-xe-may-tai-vietnam';
            } else {
                url = url + '-va-' + all_valid_filters.join('-va-');
            }

            return url;
        }

        function getOuterUnion(selectedFilters, existingFilters) {
            const union = {};
            const intersection = {};

            Object.keys(existingFilters).forEach(key => {
                if (selectedFilters[key]) {
                    // Union
                    union[key] = Array.from(new Set([...existingFilters[key], ...selectedFilters[key]]));

                    // Intersection
                    intersection[key] = existingFilters[key].filter(value => selectedFilters[key].includes(value));
                } else {
                    union[key] = existingFilters[key];
                }
            });

            // adding uncommon filters
            Object.keys(selectedFilters).forEach(key => {
                if (!union[key]) {
                    union[key] = selectedFilters[key];
                }
            });

            // removing common filters
            Object.keys(intersection).forEach(key => {
                union[key] = union[key].filter(value => !intersection[key].includes(value));
            });

            return union;
        }
    </script>
<?php
    return ob_get_clean();
}

add_shortcode('motorcycle_search_filter', 'motorcycle_search_filter_shortcode');

// require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/findnew-upcoming-cars.php';
function apply_filters_to_motorcycles($selectedFilters)
{
    enqueue_apply_filter_to_motorcycles_css();

    ob_start();

    $filter_meta = array(
        'Price' => array('post_type' => 'motorcycle-variant', 'meta_key' => 'price'),
        'Category' => array('post_type' => 'motorcycle-listing', 'meta_key' => 'listing_type'),
        'Transmission' => array('post_type' => 'motorcycle-variant', 'meta_key' => 'gearbox'),
        'Fuel' => array('post_type' => 'motorcycle-variant', 'meta_key' => 'fuel_type'),
        'Seats' => array('post_type' => 'motorcycle-variant', 'meta_key' => 'seat'),
        'Displacement' => array('post_type' => 'motorcycle-variant', 'meta_key' => 'capacity'),
    );


    /** logic to get listing ids */
    $intersected_listing_ids = array();
    $selected_filters_keys = array_keys($selectedFilters);
    foreach ($filter_meta as $key => $value) {
        if (in_array($key, $selected_filters_keys)) {
            $post_type = $value['post_type'];
            $meta_key = $value['meta_key'];
            $category_filter_values = $selectedFilters[$key];

            $category_listing_ids = array();
            foreach ($category_filter_values as $category_filter_value) {
                $listing_ids = get_motorcycle_listing_ids_of_filter($meta_key, $category_filter_value, $post_type);
                $category_listing_ids = array_merge($category_listing_ids, $listing_ids);
                $category_listing_ids = array_unique($category_listing_ids);
            }

            // take intersection of all listing ids
            if (count($intersected_listing_ids) == 0) {
                $intersected_listing_ids = $category_listing_ids;
            } else {
                $intersected_listing_ids = array_intersect($intersected_listing_ids, $category_listing_ids);
            }
        }
    }

    if (count($intersected_listing_ids) == 0) {
        return '<h1 class="wa-title-text">No motorcycles found</h1>';
    }

    $formatted_motorcycle_data = format_bike_response($intersected_listing_ids);
?>
    <h1 class='wa-title-text'><?php echo get_motorcycle_filter_title(); ?></h1>
    <h2 class='wa-title-text'><?php echo count($intersected_listing_ids); ?> motorcycles found</h2>
    <div>
        <?php
        display_popular_bike_posts($formatted_motorcycle_data);
        echo do_shortcode('[findnew_bike_comparison]');
        echo do_shortcode('[motor_related_news]');
        ?>
    </div>
<?php

    $output = ob_get_clean();
    echo $output;
}

function get_existing_motorcycle_filters()
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

    $current_url = $_SERVER['REQUEST_URI'];
    $current_url = rtrim($current_url, '/');
    $path_parts = explode('/', $current_url);
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

    //     $parts = explode('-', $last_part);
    $existing_filters = [];
    foreach ($parts as $part) {
        if (isset($filter_mapping[$part])) {
            // seggregate by filter category
            $existing_filters[$filter_mapping[$part]['filter-type']][] = $filter_mapping[$part]['filter-value'];
        }
    }

    return $existing_filters;
}


function get_motorcycle_listing_ids_of_filter($filter_type, $filter_value, $post_type = 'variant')
{
    $cache_key = 'motorcycle_listing_ids_based_on_filter_' . $filter_type . '_' . $filter_value;

    if (!FETCH_FROM_DB && USE_REDIS_CACHE) {
        $listing_ids = get_data_from_redis($cache_key);
        if ($listing_ids) {
            return $listing_ids;
        }
    }

    if ($post_type == 'motorcycle-variant') {
        $listing_ids = get_motorcycle_listing_ids_based_on_variant_filter($filter_type, $filter_value);
    } else {
        $listing_ids = get_motorcycle_listing_ids_based_on_listing_filter($filter_type, $filter_value);
    }

    set_data_to_redis($cache_key, $listing_ids);

    return $listing_ids;
}


function get_motorcycle_listing_ids_based_on_variant_filter($filter_type, $filter_value)
{
    $meta_query = array();
    // if filter type is retail price, construct meta query for price range
    if ($filter_type == 'price') {
        // remove giua-vnd- from the filter value
        $filter_value = str_replace('giua-vnd-', '', $filter_value);
        // check if the filter value contains -tr or -ty and set the price range accordingly
        $filter_value = str_replace('giua-vnd-', '', $filter_value);
        $is_tr = strpos($filter_value, '-tr') !== false;

        $filter_value = str_replace('-tr', '', $filter_value);
        $filter_value = str_replace('-ty', '', $filter_value);
        $filter_value = explode('-', $filter_value);
        $min_price = (int) $filter_value[0];
        $max_price = (int) $filter_value[1];

        $price_range = array();
        $price_range[] = $min_price * 1000 * ($is_tr ? 1 : 1000);
        $price_range[] = $max_price * 1000 * ($is_tr ? 1 : 1000);

        $meta_query = array(
            'key' => 'price',
            'value' => $price_range,
            'type' => 'NUMERIC',
            'compare' => 'BETWEEN'
        );
    } else {
        $meta_query = array(
            'key' => $filter_type,
            'value' => $filter_value,
            'compare' => 'like'
        );
    }

    // get post parent ids based on variant filter
    $args = array(
        'post_type' => 'motorcycle-variant',
        'meta_query' => array($meta_query),
        'posts_per_page' => -1
    );

    $variants = get_posts($args);
    $listing_ids = array();
    foreach ($variants as $variant) {
        $listing_ids[] = $variant->post_parent;
    }

    return $listing_ids;
}


function get_motorcycle_listing_ids_based_on_listing_filter($filter_type, $filter_value)
{
    // if listing_type, get listing ids based on body type
    if ($filter_type == '_listing_type') {
        $filter_value = $filter_value;

        if (!$filter_value) {
            return [];
        }
    }

    $args = array(
        'post_type' => 'motorcycle-listing',
        'meta_query' => array(
            array(
                'key' => $filter_type,
                'value' => $filter_value,
                'compare' => '='
            )
        ),
        'posts_per_page' => -1,
        'fields' => 'ids'
    );

    $listings = get_posts($args);
    $listing_ids = $listings;

    return $listing_ids;
}

function get_motorcycle_filter_title()
{
    $current_url = trim($_SERVER['REQUEST_URI'], '/');
    $last_part = basename($current_url);
    $current_year = date('Y');

    // if the last part has tot-nhat- and -xe-may-tai-vietnam, remove them
    if (strpos($last_part, 'tot-nhat-') !== false && strpos($last_part, '-xe-may-tai-vietnam') !== false) {
        $last_part = str_replace(['tot-nhat-', '-xe-may-tai-vietnam'], '', $last_part);
        $parts = strpos($last_part, '-va-') === false ? [$last_part] : explode('-va-', $last_part);
    } else {
        // remove tot-nhat-va- from the last part
        $last_part = str_replace('tot-nhat-va-', '', $last_part);
        $parts = strpos($last_part, '-va-') === false ? [$last_part] : explode('-va-', $last_part);
    }

    $filters = [];
    $price_range_text = '';

    foreach ($parts as $part) {
        if (strpos($part, 'giua') === 0) {
            // Handle price range ("between-php-120-1500k")
            $price_range = str_replace('giua-vnd-', '', $part);
            $price_range_text = "between VND $price_range";
        } else {
            // Convert URL-friendly word to sentence-friendly word
            $filters[] = ucfirst($part);
        }
    }

    // Combine filters and price range to form the title
    $filters_text = implode(', ', $filters);
    $title = "$current_year Best New $price_range_text $filters_text Motorcycles in Vietnam";

    return $title;
}
