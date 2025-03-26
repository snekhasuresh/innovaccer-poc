<?php
// import search-filter-shortcode.css
function enqueue_search_filter_shortcode_css()
{
    wp_enqueue_style('search-filter-shortcode', get_stylesheet_directory_uri() . '/widget-shortcodes/new-cars/css/search-filter-shortcode.css');
}

// import apply-filter-to-cars.css
function enqueue_apply_filter_to_cars_css()
{
    wp_enqueue_style('apply-filter-to-cars', get_stylesheet_directory_uri() . '/widget-shortcodes/new-cars/css/apply-filter-to-cars.css');
}

function car_search_filter_shortcode()
{
    enqueue_search_filter_shortcode_css();

    // price range
     $price_ranges = [
        'all' => 'All',
        '0-300' => '0 - 300 triệu',
        '300-500' => '300 - 500 triệu',
        '500-700' => '500 - 700 triệu',
        '700-1000' => '700 triệu - 1 tỷ',
        '1000-2000' => '1 - 2 tỷ',
        '2000-4000' => '2 - 4 tỷ',
        '4000-7000' => '4 - 7 tỷ',
        '7000-10000' => '7 - 10 tỷ',
        '10000-50000' => '10 - 50 tỷ',
    ];

    // body type
    $body_type_terms = get_terms(array('taxonomy' => 'listing_type', 'hide_empty' => false));
    $body_types = [];
    $body_types['all'] = 'All';
    $body_types_slug = [];
    foreach ($body_type_terms as $term) {
        $body_types[$term->term_id] = $term->name;
        $body_types_slug[$term->term_id] = $term->slug;
    }

    // Segment
    $segments = [
        'all' => 'All',
        'A-Segment' => 'A-Segment',
        'B-Segment' => 'B-Segment',
        'C-Segment' => 'C-Segment',
        'D-Segment' => 'D-Segment',
        'E-Segment' => 'E-Segment',
        'Commercial' => 'Commercial',
        'Executive' => 'Executive',
        'Grand Tourer' => 'Grand Tourer',
        'Luxury' => 'Luxury',
        'sports-car' => 'Sports Car',
        'Super Car' => 'Super Car',
        'Compact Executive' => 'Compact Executive',
        '4x4' => '4x4',
        '4x2' => '4x2'
    ];

    // transmission
    $transmissions = [
        'all' => 'All',
        'mt' => 'MT',
        'amt' => 'AMT',
        'cvt' => 'CVT',
        'dct' => 'DCT',
        'at' => 'AT',
        'mct' => 'MCT',
        'ev' => 'EV',
        'e-cvt' => 'E-CVT'
    ];

    // fuel
    $fuels = [
        'all' => 'All',
        'petrol' => 'Petrol',
        'diesel' => 'Diesel',
        'petrol-hybrid' => 'Petrol Hybrid',
        'diesel-hybrid' => 'Diesel Hybrid',
        'ev' => 'Electric Vehicle (EV)'
    ];

    // seats
    $seats = [
        'all' => 'All',
        '2' => '2 seater',
        '4' => '4 seater',
        '5' => '5 seater',
        '6' => '6 seater',
        '7' => '7 seater',
        '8' => '8 seater',
        '9' => '9 seater',
    ];

    // drive type
    $drive_types = [
        'all' => 'All',
        'frontwheeldrive' => 'FWD',
        'rearwheeldrive' => 'RWD',
        'allwheeldrive' => 'AWD'
    ];

    $all_filters = [];
    $all_filters['price_range'] = array('label' => 'Price', 'values' => $price_ranges);
    $all_filters['body_type'] = array('label' => 'Body Type', 'values' => $body_types);
    $all_filters['segment'] = array('label' => 'Segment', 'values' => $segments);
    $all_filters['transmission'] = array('label' => 'Transmission', 'values' => $transmissions);
    $all_filters['fuel'] = array('label' => 'Fuel', 'values' => $fuels);
    $all_filters['seats'] = array('label' => 'Seats', 'values' => $seats);
    $all_filters['drive_type'] = array('label' => 'Drive Type', 'values' => $drive_types);
    ob_start();
    $translate = [

        'Search Filters' => 'Bộ lọc tìm kiếm',
        'More Options' => 'Lựa chọn khác',
        'Close Options' => 'Đóng các tùy chọn',
    ];


?>
    <div class="car-search-filters">
        <h2 class="wa-title-text"><?php echo $translate['Search Filters']; ?></h2>
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
    <div class="more-options-container">
        <div class="line"></div>
        <button class="more-options-toggle">
            <?php echo $translate['More Options']; ?> <span style="margin-left: 5px;"><i class="fas fa-chevron-down"></i></span>
        </button>
        <div class="line"></div>
    </div>
    <div id="selected-filters-container">
    </div>

    <script type="text/javascript">
        document.addEventListener('DOMContentLoaded', function() {
            const moreOptionsToggle = document.querySelector('.more-options-toggle');
            const filterGroups = document.querySelectorAll('.filter-group');
            const moreOptionsText = 'Lựa chọn khác <span style="margin-left: 5px;"><i class="fas fa-chevron-down"></i></span>';
            const closeOptionsText = 'Đóng các tùy chọn <span style="margin-left: 5px;"><i class="fas fa-chevron-up"></i></span>';

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
                        if (index >= 5) {
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
        });


        let selectedFilters = {};
        var slug_map = slug_map || {};
        Object.assign(slug_map, <?php echo json_encode($body_types_slug); ?>);
        Object.assign(slug_map, {
            // price range
            '0-300': 'giua-vnd-0-300-tr',
            '300-500': 'giua-vnd-300-500-tr',
            '500-700': 'giua-vnd-500-700-tr',
            '700-1000': 'giua-vnd-700-1-ty',
            '1000-2000': 'giua-vnd-1-2-ty',
            '2000-4000': 'giua-vnd-2-4-ty',
            '4000-7000': 'giua-vnd-4-7-ty',
            '7000-10000': 'giua-vnd-7-10-ty',
            '10000-50000': 'giua-vnd-10-50-ty',

            // body types
            'Sedan': 'sedan',
            'Hatchback': 'hatchback',
            'SUV': 'suv',
            'Coupe': 'coupe',
            'Convertible': 'convertible',
            'Minivan': 'minivan',
            'Pickup Truck': 'pickup-truck',
            'Van': 'van',

            // segments
            'A-Segment': 'a-segment',
            'B-Segment': 'b-segment',
            'C-Segment': 'c-segment',
            'D-Segment': 'd-segment',
            'E-Segment': 'e-segment',
            'Commercial': 'commercial',
            'Executive': 'executive',
            'Grand Tourer': 'grand-tourer',
            'Luxury': 'luxury',
            'Sports Car': 'sports-car',
            'Super Car': 'super-car',
            'Compact Executive': 'compact-executive',
            '4x4': '4x4',
            '4x2': '4x2',

            // transmissions
            'mt': 'mt',
            'amt': 'amt',
            'cvt': 'cvt',
            'dct': 'dct',
            'at': 'at',
            'mct': 'mct',
            'ev': 'ev',
            'e-cvt': 'eCvt',

            // fuels
            'petrol': 'petrol',
            'diesel': 'diesel',
            'petrol-hybrid': 'petrol-hybrid',
            'diesel-hybrid': 'diesel-hybrid',
            'ev': 'ev',

            // seats
            '2': '2',
            '4': '4',
            '5': '5',
            '6': '6',
            '7': '7',
            '8': '8',
            '9': '9',
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
            // if the last part has tot-nhat- and -oto-tai-vietnam, remove them
            if (filter.includes('tot-nhat-') && filter.includes('-oto-tai-vietnam')) {
                filter = filter.replace('tot-nhat-', '');
                filter = filter.replace('-oto-tai-vietnam', '');
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
            // if url is baseurl/xe-oto
            if (filter_parts.length === 1 && filter_parts[0] === 'xe-oto') {
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
                let existingFilters = <?php echo json_encode(get_existing_filters()); ?>;
                if (!existingFilters) {
                    existingFilters = {};
                }
                if (value === 'all') {
                    delete selectedFilters[filter];
                    delete existingFilters[filter];
                }
                var url = constructURL(selectedFilters, existingFilters);
				console.log(url);
//                 window.location.href = url;
            })
        });

        function constructURL($selectedFilters, existingFilters) {
            let outerUnion = getOuterUnion($selectedFilters, existingFilters);
            outerUnion = Object.fromEntries(Object.entries(outerUnion).filter(([_, v]) => v.length > 0));

            if (Object.keys(outerUnion).length === 0) {
                return "<?php echo home_url('/xe-oto'); ?>";
            }

            const base_url = "<?php echo home_url('/'); ?>" + 'xe-hoi-moi/tot-nhat';
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
                url = url + '-' + all_valid_filters[0] + '-oto-tai-vietnam';
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

add_shortcode('car_search_filter', 'car_search_filter_shortcode');

// require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/findnew-upcoming-cars.php';
function apply_filters_to_cars($selectedFilters)
{
    enqueue_apply_filter_to_cars_css();

    ob_start();

    $filter_meta = array(
        'Price' => array('post_type' => 'variant', 'meta_key' => 'retail_price'),
        'Body Type' => array('post_type' => 'listing', 'meta_key' => '_listing_type'),
        'Segment' => array('post_type' => 'listing', 'meta_key' => 'listing-segment'),
        'Transmission' => array('post_type' => 'variant', 'meta_key' => 'transmission'),
        'Fuel' => array('post_type' => 'variant', 'meta_key' => 'fuel_type'),
        'Seats' => array('post_type' => 'variant', 'meta_key' => 'seats'),
        'Drive Type' => array('post_type' => 'variant', 'meta_key' => 'driven_wheels'),
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
                $listing_ids = get_listing_ids_of_filter($meta_key, $category_filter_value, $post_type);
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
        return '<h1 class="wa-title-text">No cars found</h1>';
    }

    $formatted_car_data = format_car_response($intersected_listing_ids);
?>
    <h1 class='wa-title-text'><?php echo get_filter_title(); ?></h1>
    <h2 class='wa-title-text'><?php echo count($intersected_listing_ids); ?> cars found</h2>
    <div>
        <?php
        display_popular_car_posts($formatted_car_data);
        echo do_shortcode('[findnew_car_comparison]');
        echo do_shortcode('[car_related_news]');
        ?>
    </div>
<?php

    $output = ob_get_clean();
    echo $output;
}

function get_existing_filters()
{
    $body_type_terms = get_terms(array('taxonomy' => 'listing_type', 'hide_empty' => false));
    $filter_mapping = [];

    $filter_mapping = [
        // giua-vnd-300-500-tr
        'giua-vnd-0-300-tr' => ['filter-type' => 'Price', 'filter-value' => '0-300'],
        'giua-vnd-300-500-tr' => ['filter-type' => 'Price', 'filter-value' => '300-500'],
        'giua-vnd-500-700-tr' => ['filter-type' => 'Price', 'filter-value' => '500-700'],
        'giua-vnd-700-1-ty' => ['filter-type' => 'Price', 'filter-value' => '700-1000'],
        'giua-vnd-1-2-ty' => ['filter-type' => 'Price', 'filter-value' => '1000-2000'],
        'giua-vnd-2-4-ty' => ['filter-type' => 'Price', 'filter-value' => '2000-4000'],
        'giua-vnd-4-7-ty' => ['filter-type' => 'Price', 'filter-value' => '4000-7000'],
        'giua-vnd-7-10-ty' => ['filter-type' => 'Price', 'filter-value' => '7000-10000'],
        'giua-vnd-10-50-ty' => ['filter-type' => 'Price', 'filter-value' => '10000-50000'],


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
    ];

    foreach ($body_type_terms as $term) {
        $term_name = strtolower($term->name);
        $term_id = (string) $term->term_id;
        $filter_mapping[$term_name] = ['filter-type' => 'Body Type', 'filter-value' => $term_id];
    }

    $current_url = $_SERVER['REQUEST_URI'];
    $current_url = rtrim($current_url, '/');
    $path_parts = explode('/', $current_url);
    $last_part = end($path_parts);

    // if the last part has tot-nhat- and -oto-tai-vietnam, remove them
    if (strpos($last_part, 'tot-nhat-') !== false && strpos($last_part, '-oto-tai-vietnam') !== false) {
        $last_part = str_replace(['tot-nhat-', '-oto-tai-vietnam'], '', $last_part);
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


function get_listing_ids_of_filter($filter_type, $filter_value, $post_type = 'variant')
{
    $cache_key = 'listing_ids_based_on_filter_' . $filter_type . '_' . $filter_value;

    if (!FETCH_FROM_DB && USE_REDIS_CACHE) {
        $listing_ids = get_data_from_redis($cache_key);
        if ($listing_ids) {
            return $listing_ids;
        }
    }

    if ($post_type == 'variant') {
        $listing_ids = get_listing_ids_based_on_variant_filter($filter_type, $filter_value);
    } else {
        $listing_ids = get_listing_ids_based_on_listing_filter($filter_type, $filter_value);
    }

    set_data_to_redis($cache_key, $listing_ids);

    return $listing_ids;
}


function get_listing_ids_based_on_variant_filter($filter_type, $filter_value)
{
    $meta_query = array();
    // if filter type is retail price, construct meta query for price range
    if ($filter_type == 'retail_price') {
        // between-rm-40-60k, get min and max price
        // remove giua-vnd- from the filter value
        $filter_value = str_replace('giua-vnd-', '', $filter_value);
        // check if the filter value contains -tr or -ty and set the price range accordingly
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
            'key' => 'retail_price',
            'value' => $price_range,
            'type' => 'NUMERIC',
            'compare' => 'BETWEEN'
        );
    } else {
        $meta_query = array(
            'key' => $filter_type,
            'value' => $filter_value,
            'compare' => '='
        );
    }

    // get post parent ids based on variant filter
    $args = array(
        'post_type' => 'variant',
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


function get_listing_ids_based_on_listing_filter($filter_type, $filter_value)
{
    // if listing_type, get listing ids based on body type
    if ($filter_type == '_listing_type') {
        $filter_value = $filter_value;

        if (!$filter_value) {
            return [];
        }
    }

    $args = array(
        'post_type' => 'listing',
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

function get_filter_title()
{
    $current_url = trim($_SERVER['REQUEST_URI'], '/');
    $last_part = basename($current_url);
    $current_year = date('Y');

    // if the last part has tot-nhat- and -oto-tai-vietnam, remove them
    if (strpos($last_part, 'tot-nhat-') !== false && strpos($last_part, '-oto-tai-vietnam') !== false) {
        $last_part = str_replace(['tot-nhat-', '-oto-tai-vietnam'], '', $last_part);
        $parts = strpos($last_part, '-va-') === false ? [$last_part] : explode('-va-', $last_part);
    } else {
        // remove tot-nhat-va- from the last part
        $last_part = str_replace('tot-nhat-va-', '', $last_part);
        $parts = strpos($last_part, '-va-') === false ? [$last_part] : explode('-va-', $last_part);
    }

    $filters = [];
    $price_range_text = '';

    foreach ($parts as $part) {
        if (strpos($part, 'giua-vnd') === 0) {
            // Handle price range giua-vnd-0-300-tr, giua-vnd-1-2-ty
            $price_range = str_replace('giua-vnd-', '', $part);
            $price_range_text = "giua VND $price_range";
        } else {
            // Convert URL-friendly word to sentence-friendly word
            $filters[] = ucfirst($part);
        }
    }

    // Combine filters and price range to form the title
    $filters_text = implode(', ', $filters);
    $title = "$current_year Best New $price_range_text $filters_text Cars in Vietnam";

    return $title;
}
