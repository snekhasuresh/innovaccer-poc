<?php
global $filter_keys;
function bike_search_filter_shortcode()
{
    // price range
    $price_ranges = [
        'all' => 'All',
        '0-40' => '0-40K',
        '40-60' => '40-60K',
        '60-90' => '60-90K',
        '90-120' => '90-120K',
        '120-150' => '120-150K',
        '150-200' => '150-200K',
        '200-300' => '200-300K',
        '300-400' => '300-400K',
        '400-600' => '400-600K',
        '600-3000' => '600-3000K'
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
        'Sports Car' => 'Sports Car',
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
        'frontWheelDrive' => 'FWD',
        'rearWheelDrive' => 'RWD',
        'allWheelDrive' => 'AWD'
    ];

    $all_filters = [];
    $all_filters['price_range'] = array('label' => 'ราคา', 'values' => $price_ranges);
    $all_filters['body_type'] = array('label' => 'Body Type', 'values' => $body_types);
    $all_filters['segment'] = array('label' => 'Segment', 'values' => $segments);
    $all_filters['transmission'] = array('label' => 'Transmission', 'values' => $transmissions);
    $all_filters['fuel'] = array('label' => 'Fuel', 'values' => $fuels);
    $all_filters['seats'] = array('label' => 'Seats', 'values' => $seats);
    $all_filters['drive_type'] = array('label' => 'Drive Type', 'values' => $drive_types);
    ob_start();
?>
    <div class="car-search-filters">
        <h2 class="wa-title-text">กรองผลการค้นหา</h2>
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
            More Options <span style="margin-left: 5px;"><i class="fas fa-chevron-down"></i></span>
        </button>
        <div class="line"></div>
    </div>
    <div id="selected-filters-container">
    </div>


    <style>
        .nav-tabs li a {
            margin-right: 2px;
            font-size: 16px;
            font-weight: 700;
            line-height: 1.9;
            border: none;
            /* Remove all side borders */
            border-radius: 8px 8px 0 0;
        }

        .nav-link:hover {
            border-bottom: 3px solid #feb429;
            color: #262626;
            background-color: white !important;
            cursor: pointer;
        }


        .nav-link {
            color: #8c8c8c;
        }

        .nav-link:hover {
            border: none;
            /* No border on hover */
        }

        .nav-link.active {
            color: #262626 !important;
            border-bottom: 3px solid #feb429 !important;
            /* Only bottom border */
            background-color: transparent !important;
            border-left: none !important;
            border-right: none !important;
            border-top: none !important;
        }

        #selectedFiltersContainer {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-bottom: 10px;
        }

        #selectedFiltersContainer div {
            display: flex;
            align-items: center;
            position: relative;
        }

        .car-search-filters {
            font-family: Arial, sans-serif;
            margin-bottom: 20px;
            margin-top: 43px;
        }



        .heading-search-filters {
            font-size: 26px;
            line-height: 32px;

            font-weight: bold;
            color: black;
        }

        .filter-group {
            margin-bottom: 10px;
            display: flex;
            font-size: 14px;
            font-weight: 500;

        }

        .filter-group-container {
            margin-top: 30px;
        }

        .filter-group label {
            flex: 0 0 124px;
            margin-right: 10px;
            text-align: left;
            font-size: 14px;
            font-weight: 500;
            /* Align the label text to the right */
        }


        .filter-group ul {
            list-style: none;
            padding: 0;
            margin: 0;
            display: flex;
            flex-wrap: wrap;
        }

        .filter-group ul li {
            margin-right: 8px;
            /* Reduced margin */
            margin-bottom: 8px;
        }

        .filter-group button {

            /* Reduced padding to make buttons smaller */
            font-size: 14px;
            font-family: "roboto";
            /* Reduced button text size */
            background: none;

            border: none;
            cursor: pointer;
            border-radius: 5px;

            padding-left: 11px;
            padding-right: 11px;
            font-weight: 400;
            /* Reduced button height */
        }

        .filter-group button.highlight-yellow {
            display: block;
            background: #feb429;
            color: #fff;
        }

        .filter-group button.highlight-yellow:hover,
        .filter-group button.highlight-dark-yellow:hover {
            background-color: #feb429;
            color: white;
        }

        .filter-group button.highlight-dark-yellow {
            background-color: #feb429;
            color: black;
        }

        .filter-group button:hover {
            background-color: #f5f5f5;
        }


        .more-options-container {
            display: flex;
            justify-content: center;
            align-items: center;
            margin-top: -30px;
        }

        .more-options-toggle {
            cursor: pointer;
            padding: 4px 8px;
            font-size: 12px;
            background-color: white;
            border: 1px solid #ccc;
            width: 227px;
            height: 40px;
            margin-top: 39px;
            border-top: none;
            background-color: white;
            font-family: "Roboto";
            font-weight: 700;
            line-height: 20px;
            font-size: 14px;
            color: #595959;

        }


        .line {
            height: 0.5px;
            background-color: #ccc;
            flex-grow: 1;
        }

        .selected-filters {
            border: 1px solid #e0e0e0;
            padding: 10px;
            border-radius: 5px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 63px;
            background-color: #f9f9f9;
            margin-top: 28px;
        }

        .selected-filters h4 {
            font-size: 16px;
            margin-right: 10px;
        }

        .selected-filters .filter-tag {
            background-color: #feb429;
            color: white;
            padding: 5px 10px;
            border-radius: 3px;
            margin-right: 5px;
            display: inline-flex;
            align-items: center;
            cursor: pointer;
        }

        .selected-filters .filter-tag span {
            margin-right: 5px;
        }

        .selected-filters .filter-tag .remove-filter {
            background-color: #ccc;
            color: #fff;
            padding: 2px 5px;
            border-radius: 50%;
            font-size: 10px;
            cursor: pointer;
        }

        #resetFilters {
            border: none;
            background-color: #F9F9F9;

            cursor: pointer;

        }

        .reset-filters {
            display: flex;
            justify-content: flex-end;
            padding: auto;
            margin: 10px 0px;
        }
    </style>

    <script type="text/javascript">
        document.addEventListener('DOMContentLoaded', function() {
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
            '0-40': 'between0to40K',
            '40-60': 'between40to60K',
            '60-90': 'between60to90K',
            '90-120': 'between90to120K',
            '120-150': 'between120to150K',
            '150-200': 'between150to200K',
            '200-300': 'between200to300K',
            '300-400': 'between300to400K',
            '400-600': 'between400to600K',
            '600-3000': 'between600to3000K',

            // body types
            'Sedan': 'sedan',
            'Hatchback': 'hatchback',
            'SUV': 'suv',
            'Coupe': 'coupe',
            'Convertible': 'convertible',
            'Minivan': 'minivan',
            'Pickup Truck': 'pickupTruck',
            'Van': 'van',

            // segments
            'A-Segment': 'aSegment',
            'B-Segment': 'bSegment',
            'C-Segment': 'cSegment',
            'D-Segment': 'dSegment',
            'E-Segment': 'eSegment',
            'Commercial': 'commercial',
            'Executive': 'executive',
            'Grand Tourer': 'grandTourer',
            'Luxury': 'luxury',
            'Sports Car': 'sportsCar',
            'Super Car': 'superCar',
            'Compact Executive': 'compactExecutive',
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
            'petrol-hybrid': 'petrolHybrid',
            'diesel-hybrid': 'dieselHybrid',
            'ev': 'ev',

            // seats
            '2': '2Seater',
            '4': '4Seater',
            '5': '5Seater',
            '6': '6Seater',
            '7': '7Seater',
            '8': '8Seater',
            '9': '9Seater',

            // drive types
            'frontWheelDrive': 'fwd',
            'rearWheelDrive': 'rwd',
            'allWheelDrive': 'awd'
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
            var filter_parts = filter.split('-');
            all_filters = <?php echo json_encode($all_filters); ?>;

            // check if filter_parts contains 'best'
            var valid_url = filter_parts.length > 1 && filter_parts[0] === 'best';
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
            if (filter_parts.length === 1 && filter_parts[0] === 'cars') {
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
                let existingFilters = <?php echo json_encode(motor_get_existing_filters()); ?>;
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
                return "<?php echo home_url('/cars'); ?>";
            }

            let base_url = "<?php echo home_url('/'); ?>" + 'new-cars/best';
            for (const [key, value] of Object.entries(outerUnion)) {
                if (value.length > 0) {
                    for (var i = 0; i < value.length; i++) {
                        if (slug_map[value[i]] !== undefined) {
                            base_url = base_url + '-' + slug_map[value[i]];
                        }
                    }
                }
            }
            return base_url;
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


add_action('wp_ajax_handle_bike_search_filter', 'handle_bike_search_filter');
add_action('wp_ajax_nopriv_handle_bike_search_filter', 'handle_bike_search_filter');
function handle_bike_search_filter()
{
    if (isset($_POST['filter']) && isset($_POST['value'])) {
        ob_start();

        if (isset($_POST['selectedFilters'])) {
            apply_filters_to_bikes($_POST['selectedFilters']);
        } else {
            apply_filters_to_bikes(array());
        }

        $output = ob_get_clean();
        wp_send_json_success($output);
    } else {
        wp_send_json_error('Invalid request');
    }
}

add_shortcode('bike_search_filter', 'bike_search_filter_shortcode');

// require_once ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/new-cars/findnew-upcoming-cars.php';

function apply_filters_to_bikes($selectedFilters)
{
    ob_start();

    $args = array('post_type' => 'listing', 'meta_query' => array(), 'posts_per_page' => -1);

    echo '<div class="widget-container"> </div>';

    $need_variant_query = false;
    $filter_meta = array(
        'Price' => array('post_type' => 'variant', 'meta_key' => 'retail_price'),
        'Body Type' => array('post_type' => 'listing', 'meta_key' => 'listing_type'),
        'Segment' => array('post_type' => 'listing', 'meta_key' => 'listing-segment'),
        'Transmission' => array('post_type' => 'variant', 'meta_key' => 'transmission'),
        'Fuel' => array('post_type' => 'variant', 'meta_key' => 'fuel_type'),
        'Seats' => array('post_type' => 'variant', 'meta_key' => 'seats'),
        'Drive Type' => array('post_type' => 'variant', 'meta_key' => 'driven_wheels'),
    );

    // get variant post type labels from $filter_meta
    foreach ($filter_meta as $key => $value) {
        if (in_array($key, array_keys($selectedFilters)) && $value['post_type'] == 'variant') {
            $need_variant_query = true;
        }
    }

    if ($need_variant_query) {
        $variant_args = array('post_type' => 'variant', 'meta_query' => array(), 'posts_per_page' => -1);
        $variant_args['meta_query'] = array('relation' => 'AND');
        foreach ($selectedFilters as $key => $value) {
            if ($filter_meta[$key]['post_type'] == 'variant') {
                // if key is not Price and post type is variant
                if ($key !== 'Price') {
                    $variant_args['meta_query'][] = array(
                        'key' => $filter_meta[$key]['meta_key'],
                        'value' => $value,
                        'compare' => 'IN'
                    );
                    continue;
                } else {

                    $price_meta_args = ['relation' => 'OR'];

                    //  check that the retail_price meta key exists.
                    $variant_args['meta_query'][] = array(
                        'key' => 'retail_price',
                        'compare' => 'EXISTS',
                    );
                    foreach ($value as $price) {
                        $value = explode('-', $price);
                        $value[0] = intval($value[0]) * 1000;
                        $value[1] = intval($value[1]) * 1000;
                        $price_meta_args[] = array(
                            'key' => $filter_meta[$key]['meta_key'],
                            'type' => 'NUMERIC',
                            'value' => $value,
                            'compare' => 'BETWEEN'
                        );
                    }

                    $variant_args['meta_query'][] = $price_meta_args;
                    continue;
                }
            }
        }

        $variant_args['meta_query'][] = [
            'key' => 'on_sale',
            'value' => 'Yes',
            'compare' => '=='
        ];

        $variants = get_posts($variant_args);
        if (count($variants) > 0) {
            // get all listing ids from $variants
            $listing_ids = array();
            foreach ($variants as $variant) {
                $listing_ids[] = $variant->post_parent;
            }
            // remove duplicates from $listing_ids
            $listing_ids = array_unique($listing_ids);
            $args['post__in'] = $listing_ids;
        } else {
            $args['post__in'] = array();
        }
    }

    // add queries to $args
    if (in_array('Body Type', array_keys($selectedFilters))) {
        // get term ids of Body Type
        $term_ids = array();
        foreach ($selectedFilters['Body Type'] as $key => $value) {
            $term = get_term_by('id', $value, 'listing_type');
            $term_ids[] = $term->term_id;
        }

        $args['meta_query'][] = array(
            'key' => '_listing_type',
            'value' => $term_ids,
            'compare' => 'IN'
        );
    }

    if (in_array('Segment', array_keys($selectedFilters))) {
        $args['meta_query'][] = array(
            'key' => 'listing-segment',
            'value' => $selectedFilters['Segment'],
            'compare' => 'IN'
        );
    }

    /* you can use this to display the results of your search */
    /* add the shortcode here */
    if ($need_variant_query && count($args['post__in']) == 0) {
        echo 'No cars found';
    } else {
        $listings = new WP_Query($args);

        if ($listings->have_posts()) {
            echo '<h2 class="wa-title-text">' . count($listings->posts) . ' cars found</h2>';

            $listing_ids = array();
            while ($listings->have_posts()) {
                $listings->the_post();
                $listing_ids[] = get_the_ID();
            }

            // put the shortcodes here
    ?>
            <div>

                <!-- create a grid with 3 items in a row with margin of 10px  between them -->
                <div>
                    <?php
                    // display_popular_car_posts($listings->post_count > 9 ? array_slice($listings->posts, 0, 9) : $listings->posts);
                    display_popular_bike_posts($listings->posts);
                    ?>
                </div>

                <div>
                    <h2 class="wa-title-text">Compare Similar Cars</h2>
                    <?php
                    echo do_shortcode('[findnew_car_comparison post_includes="' . implode(',', $listing_ids) . '"]');
                    ?>
                </div>

                <div class='widget-container'>
                    <!-- <h2>Upcoming Cars</h2> -->
                    <?php
                    // echo do_shortcode('[upcoming_cars posts_include="' . implode(',', $listing_ids) . '"]');
                    // echo do_shortcode('[upcoming_cars]');
                    ?>
                </div>

                <div>
                    <!-- <h2>Related News</h2> -->
                    <?php
                    echo do_shortcode('[car_related_news related_listings="' . implode(',', $listing_ids) . '"]');
                    ?>
                </div>

            </div>

            <style>
                /* Tab container */
                .fruit-tabs {
                    margin: 20px 0;
                    padding: 15px;
                }

                .tab {
                    display: flex;
                    list-style: none;
                    padding: 0;
                    margin-bottom: 9px;
                }

                .tabs {
                    border-bottom: 1px solid #ddd;
                    /* Separator */
                }

                .tabs li {
                    margin-right: 25px;
                }

                .tabs .tabs-link {
                    padding: 4px 3px;
                    text-decoration: none;
                    color: #8c8c8c;
                    background-color: transparent;
                    border: none;
                    position: relative;
                    font-size: 16px;
                    cursor: pointer;
                    transition: color 0.3s;
                    font-weight: 700;
                    letter-spacing: 0.5px;
                    /* Add letter spacing */
                }

                .tab .tabs-link.active {
                    color: #262626;
                    border-bottom: 3px solid #feb429;
                    padding-bottom: 12px;
                    border-bottom-width: 3px;
                }

                /* Each car item */
                .car-item {
                    display: flex;
                    flex-direction: column;
                    align-items: flex-start;
                    text-decoration: none;
                    width: 100%;
                    max-width: 300px;
                    padding: 15px;
                    border: 1px solid #ddd;
                    border-radius: 8px;
                    background-color: white;
                    transition: transform 0.3s ease, box-shadow 0.3s ease;
                }

                .car-item:hover {
                    transform: translateY(1.01px);
                    box-shadow: 0 5px 5px rgba(0, 0, 0, 0.1);
                }

                /* Car list grid */
                .car-list {
                    display: grid;
                    grid-template-columns: repeat(3, 1fr);
                    /* 3-column grid */
                    gap: 20px;
                    width: 100%;
                }

                /* Car image */
                .car-item img {
                    width: 100%;
                    height: 124px;
                    margin-bottom: 10px;
                    border-radius: 5px;
                }

                /* Car title */
                .car-item h4 {
                    font-size: 16px;
                    color: #262626;
                    margin-bottom: -2px;
                    text-align: center;
                    font-weight: 700;
                }

                /* Car price */
                .car-item p {
                    font-size: 14px;
                    color: #576b95;
                    text-align: center;
                    font-weight: 700;
                    margin-bottom: 10px;
                }

                /* Hide all panes initially */
                .tab-pane {
                    display: none;
                }

                /* Show active pane */
                .tab-pane.active {
                    display: flex;
                    flex-direction: column;
                }

                .view-model-button {
                    background: white;
                    border: 1px solid #feb429;
                    width: 255px;
                    height: 40px;
                    border-radius: 5px;
                    font-weight: 700;
                    font-size: 14px;
                    color: #feb429;
                }

                .title-and-post {
                    text-align: start;
                    display: flex;
                    flex-direction: column;
                    align-items: flex-start;
                }
            </style>

            <style>
                .car-brand-model {
                    font-size: 16px;
                    font-weight: bold;
                    color: #262626;
                    font-family: "Roboto";
                    line-height: 22px;
                    word-break: break-word;
                    text-overflow: ellipsis;
                    display: -webkit-box;
                    -webkit-box-orient: vertical;
                    -webkit-line-clamp: 1;
                    overflow: hidden;
                }

                /* Carousel container */
                .upcoming-cars-list {
                    display: flex;
                    flex-wrap: nowrap;
                    overflow: visible;
                    position: relative;
                    gap: 20px;
                    height: 240px !important;
                }

                .slick-track {
                    display: flex;
                    gap: 20px;
                }

                .slick-prev:focus,
                .slick-next:focus,
                .slick-prev:active,
                .slick-next:active {
                    background-color: #ffffff !important;
                    color: black !important;
                    outline: none;
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15);
                }

                .slick-prev:before {
                    content: '←';
                    color: black !important;
                }

                .slick-next:before {
                    content: '→';
                    color: black !important;
                }

                /* More specific selector for slick arrows */
                .slick-prev,
                .slick-next {
                    background-color: #ffffff !important;
                    border-radius: 50%;
                    width: 50px;
                    height: 50px;
                    z-index: 10;
                    position: absolute;
                    top: 50%;
                    transform: translateY(-50%);
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, 0.15);
                    transition: background-color 0.3s ease, color 0.3s ease;
                    border: none;
                }

                .slick-prev:before,
                .slick-next:before {
                    font-size: 20px;
                    color: black !important;
                    /* Black arrow */
                }

                /* Hover state */
                .slick-prev:hover,
                .slick-next:hover {
                    background-color: white !important;
                    /* Change to yellow on hover */
                    color: white !important;
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15) !important;
                }

                /* Focus state */
                .slick-prev:focus,
                .slick-next:focus {
                    background-color: white !important;
                    /* Keep background yellow on focus */
                    color: white !important;
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15) !important;
                    outline: none;
                }

                /* Active state when button is clicked */
                .slick-prev:active,
                .slick-next:active {
                    background-color: white !important;
                    /* Keep background yellow on click */
                    color: white !important;
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15) !important;
                    outline: none;
                }

                /* Focus-visible state for keyboard users */
                .slick-prev:focus-visible,
                .slick-next:focus-visible {
                    background-color: white !important;
                    color: white !important;
                    outline: none;
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15);
                }

                /* Prevent disappearing of background by using more specific selectors */
                button.slick-prev,
                button.slick-next,
                div.slick-prev,
                div.slick-next {
                    background-color: white !important;
                    color: white !important;
                    box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15);
                }

                /* Positioning adjustments */
                .slick-prev {
                    left: -4px !important;
                }

                .slick-next {
                    right: -25px !important;
                }

                /* Individual car item */
                .car-sedan-item {
                    display: flex;
                    flex-direction: column;
                    border: 1px solid #e0e0e0;
                    width: calc(250px - 30px);
                    border-radius: 8px;
                    overflow: hidden;
                    padding: 15px;
                    margin: 0 15px;
                    position: relative;
                    transition: transform 0.3s ease, box-shadow 0.3s ease;
                }

                /* Hover effect for car items */
                .car-item:hover {
                    transform: translateY(-1px);
                    box-shadow: 0 5px 5px rgba(0, 0, 0, 0.1);
                }

                /* Thumbnail styling */
                .car-thumbnail {
                    position: relative;
                    overflow: hidden;
                    margin-top: -29px;
                }

                .car-thumbnail img {
                    width: 100%;
                    height: auto;
                    transition: transform 0.3s ease;
                }

                /* Car label */
                .car-label {
                    position: absolute;
                    top: 10px;
                    left: 10px;
                    background-color: red;
                    color: white;
                    padding: 5px 10px;
                    border-radius: 3px;
                    font-size: 12px;
                    font-weight: bold;
                }

                /* Car content styling */
                .car-content {
                    display: flex;
                    flex-direction: column;
                    align-items: flex-start;
                    padding-top: 10px;
                }

                .car-info {
                    /* display: flex; */
                    width: 100%;
                    color: black;
                    align-items: baseline;
                    /* margin-bottom: -10px; */
                }

                .car-info p {
                    margin: 0 0 1px;
                }

                .car-brand-icon {
                    color: #feb429;
                    margin-right: 5px;
                }

                .car-brand {
                    font-size: 14px;
                    font-weight: bold;
                    color: #555;
                }

                /* Price styling */
                .price {
                    margin-top: -10px;
                    font-size: 14px;
                    font-weight: bold;
                    color: #576B95;
                }

                /* Title styling */
                .car-title {
                    font-size: 18px;
                    font-weight: bold;
                    color: #333;
                    text-decoration: none;
                    display: block;
                    margin-bottom: 5px;
                }

                /* Price text styling */
                .car-price {
                    font-size: 16px;
                    color: #777;
                }

                /* View Model button */
                .view-model-button {
                    padding: 8px 12px;
                    background-color: white;
                    color: #feb429;
                    text-decoration: none;
                    border: 1px solid #feb429;
                    border-radius: 4px;
                    margin-top: auto;
                    text-align: center;
                    width: 100%;
                    transition: background-color 0.3s ease, color 0.3s ease;
                }

                .view-model-button:hover {
                    background-color: #feb429;
                    color: white;
                }
            </style>

<?php
        }
    }

    $output = ob_get_clean();
    echo $output;
}
if (!function_exists('motor_get_existing_filters')) {
    function motor_get_existing_filters()
    {
        $body_type_terms = get_terms(array('taxonomy' => 'listing_type', 'hide_empty' => false));
        $filter_mapping = [];

        $filter_mapping = [
            // 0-40K, 40-60K, 60-90K, 90-120K, 120-150K, 150-200K, 200-300K, 300-400K, 400-600K, 600-3000K
            'between0to40K' => ['filter-type' => 'Price', 'filter-value' => '0-40'],
            'between40to60K' => ['filter-type' => 'Price', 'filter-value' => '40-60'],
            'between60to90K' => ['filter-type' => 'Price', 'filter-value' => '60-90'],
            'between90to120K' => ['filter-type' => 'Price', 'filter-value' => '90-120'],
            'between120to150K' => ['filter-type' => 'Price', 'filter-value' => '120-150'],
            'between150to200K' => ['filter-type' => 'Price', 'filter-value' => '150-200'],
            'between200to300K' => ['filter-type' => 'Price', 'filter-value' => '200-300'],
            'between300to400K' => ['filter-type' => 'Price', 'filter-value' => '300-400'],
            'between400to600K' => ['filter-type' => 'Price', 'filter-value' => '400-600'],
            'between600to3000K' => ['filter-type' => 'Price', 'filter-value' => '600-3000'],

            // A-Segment, B-Segment, C-Segment, D-Segment, E-Segment, Commercial, Executive, Grand Tourer, Luxury, Sports Car, Super Car, Compact Executive, 4x4, 4x2
            'aSegment' => ['filter-type' => 'Segment', 'filter-value' => 'A-Segment'],
            'bSegment' => ['filter-type' => 'Segment', 'filter-value' => 'B-Segment'],
            'cSegment' => ['filter-type' => 'Segment', 'filter-value' => 'C-Segment'],
            'dSegment' => ['filter-type' => 'Segment', 'filter-value' => 'D-Segment'],
            'eSegment' => ['filter-type' => 'Segment', 'filter-value' => 'E-Segment'],
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
            'petrolHybrid' => ['filter-type' => 'Fuel', 'filter-value' => 'petrol-hybrid'],
            'dieselHybrid' => ['filter-type' => 'Fuel', 'filter-value' => 'diesel-hybrid'],
            'electricVehicle' => ['filter-type' => 'Fuel', 'filter-value' => 'electric-vehicle'],

            // 2 Seater, 4 Seater, 5 Seater, 6 Seater, 7 Seater, 8 Seater, 9 Seater
            '2Seater' => ['filter-type' => 'Seating Capacity', 'filter-value' => '2 Seater'],

            // Front Wheel Drive, Rear Wheel Drive, All Wheel Drive
            'fwd' => ['filter-type' => 'Drive Type', 'filter-value' => 'fwd'],
            'rwd' => ['filter-type' => 'Drive Type', 'filter-value' => 'rwd'],
            'awd' => ['filter-type' => 'Drive Type', 'filter-value' => 'awd'],
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

        $parts = explode('-', $last_part);
        $existing_filters = [];
        foreach ($parts as $part) {
            if (isset($filter_mapping[$part])) {
                // seggregate by filter category
                $existing_filters[$filter_mapping[$part]['filter-type']][] = $filter_mapping[$part]['filter-value'];
            }
        }

        return $existing_filters;
    }
}
?>