<?php

add_shortcode('car_valuation', 'display_car_valuation_shortcode');
function display_car_valuation_shortcode()
{
    // Enqueue necessary styles and scripts
    wp_enqueue_style('car-valuation-css', get_stylesheet_directory_uri() . '/widget-shortcodes/cars-for-sale/car-valuation/car-valuation.css');
    wp_enqueue_script('car-valuation-js', get_stylesheet_directory_uri() . '/widget-shortcodes/cars-for-sale/car-valuation/car-valuation.js');

    $months = array(
        'January',
        'February',
        'March',
        'April',
        'May',
        'June',
        'July',
        'August',
        'September',
        'October',
        'November',
        'December'
    );

    $kmDriven = [
        '0 - 50,000km' => 25000,
        '50,000 - 100,000km' => 75000,
        '100,000 - 150,000km' => 125000,
        '150,000 - 200,000km' => 175000,
        '200,000 - 250,000km' => 225000,
        '250,000 - 300,000km' => 275000,
        '300,000 - 350,000km' => 325000,
        '350,000 and above' => 375000
    ];

    ob_start();
?>
    <div class="add-car-container add-car-button" id="add-car-button">
        <svg class="car-icon" viewBox="0 0 1000 500" xmlns="http://www.w3.org/2000/svg">
            <path d="M800,260 C800,230 780,200 740,200 L660,200 L580,120 C560,100 530,90 500,90 L300,90 C270,90 240,100 220,120 L140,200 L60,200 C20,200 0,230 0,260 L0,340 C0,370 20,400 60,400 L100,400 C100,450 140,490 190,490 C240,490 280,450 280,400 L520,400 C520,450 560,490 610,490 C660,490 700,450 700,400 L740,400 C780,400 800,370 800,340 L800,260 Z" fill="currentColor" />
        </svg>
        <span class="add-car-text">Value Your Car</span>
    </div>


    <div class="car-popup-overlay" id="carPopup">
        <div class="car-popup-content">
            <div class="car-popup-header">
                <h2>Value Your Car</h2>
                <button class="close-popup">&times;</button>
            </div>
            <div class="car-popup-body">
                <!-- <p class="popup-subtitle">Complete The Owner Certification, Get Exclusive Benefits</p> -->

                <!-- <div class="car-selection-container">
                    <button class="select-car-button">
                        <span class="plus-icon">+</span>
                        Select Car
                    </button>
                </div> -->

                <div class="selects-car">
                    <div class="add-car">
                        <span class="plus-sign">+</span>
                    </div>
                    <div class="selected-car-details" style="text-align: center;">
                        <!-- <img class="variant-image" src="" alt="Variant Image" style="display: none; width: 200px; height: 120px; object-fit: cover; margin: 0 auto;"> -->
                        <p class="variant-name"></p>
                        <!-- <p class="variant-price"></p> -->
                        <div class="switch-btn-container">
                            <button class="switch-button" style="display: none;">Switch</button>
                        </div>
                    </div>

                    <p class="select-car-text">Select Car</p>
                    <?php show_dropdown(); ?>
                </div>

                <!-- add km driven input -->
                <!-- <div class="km-driven-container">
                    <label for="kmDriven">KM Driven <span class="required">*</span></label>
                    <input
                        type="number"
                        id="km-driven"
                        name="kmDriven"
                        placeholder="Enter KM Driven"
                        required>
                </div> -->

                <!-- km driven range - Drpdown -->
                <div class="insurance-expiry-container">
                    <label for="insuranceExpiry">KM Driven <span class="required">*</span></label>
                    <select id="km-driven" name="kmDriven" required>
                        <option value="">Select KM Driven</option>
                        <?php
                        foreach ($kmDriven as $km => $value) {
                            echo '<option value="' . $value . '">' . $km . '</option>';
                        }
                        ?>
                    </select>
                </div>

                <!-- Insurance Expiry Month - Drpdown of months -->
                <!-- <div class="insurance-expiry-container">
                    <label for="insuranceExpiry">Insurance Expiry <span class="required">*</span></label>
                    <select id="insurance-expiry" name="insuranceExpiry" required>
                        <option value="">Select Month</option>
                        <?php
                        // foreach ($months as $month) {
                        //     echo '<option value="' . $month . '">' . $month . '</option>';
                        // }
                        ?>
                    </select>
                </div> -->

                <button class="confirm-button" id="confirm-car-button">Get Valuation</button>

                <!-- car valuation display -->
                <div class="car-valuation-container" style="display: none;">
                    <span class="car-valuation-title">Car Valuation:</span>
                    <span class="car-valuation-amount"></span>
                </div>

            </div>
        </div>
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', function() {
            let selectedVariants = [];
            let currentBrandId = '';
            let selectedModels = [];
            let model_name = '';
            let isSwitching = false;

            let currentBrandName = '';
            let currentModelName = '';
            let currentYear = '';
            let currentVariantName = '';

            // add event listener on add car button
            document.getElementById('confirm-car-button').addEventListener('click', function(event) {
                event.preventDefault();
                // get km driven, month and selected variant
                const kmDriven = document.getElementById('km-driven').value;
                console.log('kmDriven: ', kmDriven);
                // const insuranceMonth = document.getElementById('insurance-expiry').value;
                const variantName = document.querySelector('.variant-name').innerText;
                const variantItemElement = document.querySelector('.variant-item');
                const variantId = variantItemElement.dataset.variantId;
                console.log('kmDriven: ', kmDriven);

                if (variantId && kmDriven && kmDriven !== '') {
                    // make an ajax call to add the car
                    jQuery.ajax({
                        url: '<?php echo admin_url('admin-ajax.php'); ?>',
                        type: 'POST',
                        data: {
                            action: 'car_valuation',
                            variantId: variantId,
                            mileage: kmDriven,
                        },
                        success: function(response) {
                            if (response.success) {
                                response = response.data.data.data;
                                minFair = Math.round(Number(response.minFair));
                                maxExcellent = Math.round(Number(response.maxExcellent));

                                // display valuation
                                //                                 document.querySelector('.km-driven-container').style.display = 'block';
                                document.querySelector('.car-valuation-container').style.display = 'block';
                                document.querySelector('.car-valuation-amount').innerText = 'RM ' + formatNumberWithCommas(minFair) + ' - RM ' + formatNumberWithCommas(maxExcellent);

                                // hide confirm-button
                                // document.getElementById('confirm-car-button').style.display = 'none';

                                // make km-driven input readonly
                                // document.getElementById('km-driven').readOnly = true;
                            }
                        },
                        error: function(error) {
                            console.error('Error:', error);
                        }
                    }).done(function(response) {
                        if (response.success) {
                            console.log('Got car valuation');
                        }
                    })
                }
            })


            document.querySelectorAll('.selects-car').forEach(function(container) {
                const addCarButton = container.querySelector('.plus-sign');
                const addCar = container.querySelector('.add-car');
                const dropdown = container.querySelector('.dropdown-menu');
                const breadcrumbElement = dropdown.querySelector('.breadcrumb');
                const selectCarText = container.querySelector('.select-car-text');
                const switchButton = container.querySelector('.switch-button');


                const selectedCarDetails = container.querySelector('.selected-car-details');
                // const variantImage = selectedCarDetails.querySelector('.variant-image');
                const variantNameElem = selectedCarDetails.querySelector('.variant-name');
                // const variantPriceElem = selectedCarDetails.querySelector('.variant-price');


                let selectedBrand = '';
                let selectedModel = '';
                let selectedYear = '';
                let selectedVariant = '';
                // Loader element (you can style this as you want)
                const loader = document.createElement('div');
                loader.className = 'loader'; // Apply your loader styles here
                loader.innerHTML = '<i class="fas fa-spinner fa-spin"></i><br>Loading....';


                addCarButton.addEventListener('click', function(event) {
                    event.stopPropagation();
                    dropdown.style.display = dropdown.style.display === 'none' || dropdown.style.display === '' ? 'block' : 'none';
                    loadbrands();
                });


                document.addEventListener('click', function(event) {
                    if (!container.contains(event.target)) {
                        dropdown.style.display = 'none';
                    }
                });


                dropdown.addEventListener('click', function(event) {
                    const item = event.target.closest('.dropdown-item');
                    if (item) {
                        if (item.classList.contains('brand-item')) {
                            const brandName = item.querySelector('.brand-name').textContent;
                            currentBrandName = brandName;

                            loadmodels(brandName);
                        } else if (item.classList.contains('model-item')) {
                            const selectedModelName = event.target.querySelector('.model-name').textContent;
                            const modelName = item.querySelector('.model-name').textContent;
                            currentModelName = modelName;

                            loadYears(currentBrandName, currentModelName);
                        } else if (item.classList.contains('year-item')) {
                            const selectedYear = event.target.querySelector('.year-name').textContent;

                            const year = item.querySelector('.year-name').textContent;
                            currentYear = year;

                            loadvariants(currentBrandName, currentModelName, currentYear);
                        } else if (item.classList.contains('variant-item')) {
                            const variantName = item.querySelector('.variant-name').textContent;
                            const variantId = item.dataset.variantId;

                            variantNameElem.textContent = variantName

                            // Hide plus-sign and "Select Car"
                            addCar.style.display = 'none';
                            selectCarText.style.display = 'none';

                            // Show switch button
                            switchButton.style.display = 'block';


                            const cancelIcon = document.createElement('span');
                            cancelIcon.textContent = 'X'; // You can use an icon or character for this
                            cancelIcon.classList.add('cancel-icon');
                            selectedCarDetails.appendChild(cancelIcon);
                            /*if (!isSwitching) {
                                // If it's not a switch event, push the new variant into the selectedVariants array
                                selectedVariants.push(response.data.data);
                            } else {
                                // If it's a switch event, replace the variant in selectedVariants at the current index
                                selectedVariants[currentCarIndex] = response.data.data;
                                console.log('Replaced variant at index', currentCarIndex, 'with', response.data.data);
                            }
                            // selectedVariants.push(response.data.data);
                            updateBreadcrumb();*/


                            // In your main logic where you create the cancel icon
                            cancelIcon.addEventListener('click', function() {
                                cancelVariant();
                            });

                            switchButton.addEventListener('click', function() {
                                switchVariant();
                            })


                            dropdown.style.display = 'none';
                        }
                    }
                });


                breadcrumbElement.addEventListener('click', function(event) {
                    const clickedText = event.target.textContent.trim();
                    console.log('clickedText', clickedText);
                    if (clickedText === 'Brand') {
                        dropdown.innerHTML = '';
                        dropdown.appendChild(breadcrumbElement);
                        loadbrands();
                    } else if (clickedText === 'Model') {
                        dropdown.innerHTML = '';
                        dropdown.appendChild(breadcrumbElement);
                        // Fetch and display model dropdown
                        loadmodels(currentBrandName);
                    } else if (clickedText === 'Year') {
                        dropdown.innerHTML = '';
                        dropdown.appendChild(breadcrumbElement);
                        // Fetch and display year dropdown
                        loadYears(currentBrandName, currentModelName);
                    }
                });

                function cancelVariant() {
                    const lastContainer = document.querySelectorAll('.selects-car')[0];
                    if (lastContainer) {
                        // lastContainer.querySelector('.variant-image').style.display = 'none';
                        lastContainer.querySelector('.variant-name').textContent = '';
                        // lastContainer.querySelector('.variant-price').textContent = '';
                        lastContainer.querySelector('.switch-button').style.display = 'none';
                        lastContainer.querySelector('.add-car').style.display = 'block';
                        lastContainer.querySelector('.select-car-text').style.display = 'block';
                        const lastCancelIcon = lastContainer.querySelector('.cancel-icon');
                        if (lastCancelIcon) {
                            lastCancelIcon.style.display = 'none'; // Hide the cancel icon for the last container
                        }
                        loadbrands();
                    }
                }

                function switchVariant() {
                    event.stopPropagation();
                    dropdown.style.display = dropdown.style.display === 'none' || dropdown.style.display === '' ? 'block' : 'none';
                    dropdown.innerHTML = '';
                    loadbrands();
                }

                function loadbrands() {
                    dropdown.innerHTML = '';
                    dropdown.appendChild(loader);
                    jQuery.ajax({
                        url: '<?php echo admin_url('admin-ajax.php'); ?>',
                        type: 'GET',
                        data: {
                            action: 'get_make'
                        },
                        success: function(response) {
                            try {
                                if (response.success) {
                                    brands = response.data;
                                    brands.forEach(function(brand) {
                                        const brandItem = document.createElement('div');
                                        brandItem.className = 'dropdown-item brand-item';
                                        brandItem.dataset.brandName = brand;
                                        brandItem.innerHTML = '<span class="brand-name">' + brand + '</span>';
                                        dropdown.appendChild(brandItem);
                                    })
                                }

                                // Reset breadcrumb selections
                                selectedBrand = '';
                                selectedModel = '';
                                selectedYear = '';
                                selectedVariant = '';
                                updateBreadcrumb();
                                dropdown.removeChild(loader);

                            } catch (error) {
                                console.error('Invalid JSON response:', error);
                            }
                        },
                        error: function(xhr, status, error) {
                            console.error('AJAX Error:', error);
                        }
                    });
                }

                function loadmodels(brandName) {
                    // dropdown.innerHTML = '';
                    dropdown.appendChild(loader);
                    jQuery.ajax({
                        url: '<?php echo admin_url('admin-ajax.php'); ?>',
                        type: 'GET',
                        data: {
                            action: 'get_model',
                            make: brandName
                        },
                        success: function(response) {
                            if (response.success) {
                                // Clear dropdown and add breadcrumb
                                dropdown.innerHTML = '';
                                dropdown.appendChild(breadcrumbElement);

                                models = response.data;
                                models.forEach(function(model) {
                                    const modelItem = document.createElement('div');
                                    modelItem.className = 'dropdown-item model-item';
                                    modelItem.dataset.modelId = model;
                                    modelItem.innerHTML = '<span class="model-name">' + model + '</span>';
                                    dropdown.appendChild(modelItem);
                                })

                                // Remove the loader and update the breadcrumb
                                loader.remove();
                                // selectedBrand = 'Brand';
                                selectedModel = 'Model';
                                selectedYear = '';
                                selectedVariant = '';
                                updateBreadcrumb();
                                dropdown.removeChild(loader);
                            }
                        },
                        error: function(xhr, status, error) {
                            console.error('AJAX Error:', error);
                        }
                    });
                }

                function loadYears(currentBrandName, selectedModelName) {
                    // dropdown.innerHTML = '';
                    dropdown.appendChild(loader);
                    jQuery.ajax({
                        url: '<?php echo admin_url('admin-ajax.php'); ?>',
                        type: 'GET',
                        data: {
                            action: 'get_year',
                            model: selectedModelName,
                            make: currentBrandName
                        },
                        success: function(response) {
                            if (response.success) {
                                years = response.data;

                                // Clear dropdown and add breadcrumb
                                dropdown.innerHTML = '';
                                dropdown.appendChild(breadcrumbElement);

                                years.forEach(function(year) {
                                    const yearItem = document.createElement('div');
                                    yearItem.className = 'dropdown-item year-item';
                                    yearItem.dataset.year = year;
                                    yearItem.innerHTML = '<span class="year-name">' + year + '</span>';
                                    dropdown.appendChild(yearItem);
                                })

                                // Remove the loader and update the breadcrumb
                                loader.remove();
                                selectedYear = 'Year';
                                selectedVariant = '';
                                updateBreadcrumb();
                                dropdown.removeChild(loader);
                            }
                        },
                        error: function(xhr, status, error) {
                            console.error('AJAX Error:', error);
                        }
                    })
                }

                function loadvariants(currentBrandName, selectedModelName, selectedYear) {
                    // dropdown.innerHTML = '';
                    dropdown.appendChild(loader);
                    jQuery.ajax({
                        url: '<?php echo admin_url('admin-ajax.php'); ?>',
                        type: 'GET',
                        data: {
                            action: 'get_variant',
                            model: selectedModelName,
                            make: currentBrandName,
                            year: selectedYear
                        },
                        success: function(response) {
                            if (response.success) {
                                variants = response.data;

                                // Clear dropdown and add breadcrumb
                                dropdown.innerHTML = '';
                                dropdown.appendChild(breadcrumbElement);

                                variants.forEach(function(variant) {
                                    const variantItem = document.createElement('div');
                                    variantItem.className = 'dropdown-item variant-item';
                                    variantItem.dataset.variantId = variant.icar_vcode;
                                    variantItem.dataset.variantName = variant.variant_facet_unique;
                                    // variantItem.dataset.modelId = variant.modelId;
                                    variantItem.innerHTML = '<span class="variant-name">' + variant.variant_facet_unique + '</span>';
                                    dropdown.appendChild(variantItem);
                                })

                                // Remove the loader and update the breadcrumb
                                loader.remove();
                                selectedVariant = 'Variant';
                                updateBreadcrumb();
                                dropdown.removeChild(loader);
                            }
                        },
                        error: function(xhr, status, error) {
                            console.error('AJAX Error:', error);
                        }
                    });
                }

                function updateBreadcrumb() {
                    let breadcrumb = 'Brand';
                    if (selectedBrand) breadcrumb += ' > ' + selectedBrand;
                    if (selectedModel) breadcrumb += ' > ' + selectedModel;
                    if (selectedYear) breadcrumb += ' > ' + selectedYear;
                    if (selectedVariant) breadcrumb += ' > ' + selectedVariant;
                    breadcrumbElement.innerHTML = breadcrumb
                        .split(' > ')
                        .map((text, index) => `<span class="breadcrumb-item">${text}</span>`)
                        .join(' > ');
                }
            });

            function formatNumberWithCommas(number) {
                number = parseFloat(number);
                return number.toLocaleString('en-US', {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 0
                });
            }
        });
    </script>
<?php
    return ob_get_clean();
}

function show_dropdown()
{
    // $car_brands = get_icarasia_makes();
    // $car_brands = get_makes_v2();
    echo '<div class="dropdown-menu">';
    echo '<div class="breadcrumb">Brand</div>';

    // foreach ($car_brands as $brand) {
    //     echo '<div class="dropdown-item brand-item" data-brand-name="' . esc_attr($brand) . '">';
    //     echo '<span class="brand-name">' . esc_html($brand) . '</span>';
    //     echo '<div class="sub-menu model-menu" style="display: none;"></div>';
    //     echo '</div>';
    // }
    echo '</div>';
}

function car_valuation_handler()
{
    $token = get_token();
    error_log('token: ' . $token);
    if (isset($_POST['mileage']) && isset($_POST['variantId'])) {
        $mileage = sanitize_text_field($_POST['mileage']);
        $variantId = sanitize_text_field($_POST['variantId']);

        $endpoint = 'https://ex-api.carlist.my/v3.0/my/en/vehicle/averageprice?icar_vcode=' . $variantId . '&mileage=' . $mileage;
        $response = wp_remote_post(
            $endpoint,
            array(
                'method' => 'GET',
                'headers' => array(
                    'token' => $token,
                    'Content-Type' => 'application/json',
                )
            )
        );

        if (is_wp_error($response)) {
            wp_send_json_error(['message' => $response->get_error_message()]);
        }

        $data = json_decode(wp_remote_retrieve_body($response), true);


        wp_send_json_success(['message' => 'Success', 'data' => $data]);
    }
    wp_send_json_error(['message' => 'Invalid request.']);
}

add_action('wp_ajax_car_valuation', 'car_valuation_handler');
add_action('wp_ajax_nopriv_car_valuation', 'car_valuation_handler');

// ajax handler for getting makes
function get_make_handler()
{
    // $makes = get_icarasia_makes();
    $makes = get_makes_v2();
    wp_send_json_success($makes['data']);
}

add_action('wp_ajax_get_make', 'get_make_handler');
add_action('wp_ajax_nopriv_get_make', 'get_make_handler');

// ajax handler for getting models
function get_model_handler()
{
    $make = isset($_GET['make']) ? sanitize_text_field($_GET['make']) : '';
    // $models = get_icarasia_models($make);
    $models = get_models_v2($make)['data'];
    wp_send_json_success($models);
}

add_action('wp_ajax_get_model', 'get_model_handler');
add_action('wp_ajax_nopriv_get_model', 'get_model_handler');

// ajax handler for getting years
function get_year_handler()
{
    $make = isset($_GET['make']) ? sanitize_text_field($_GET['make']) : '';
    $model = isset($_GET['model']) ? sanitize_text_field($_GET['model']) : '';
    // $variants = get_icarasia_variants($make, $model);
    $years = get_years_v2($make, $model)['data'];
    wp_send_json_success($years);
}

add_action('wp_ajax_get_year', 'get_year_handler');
add_action('wp_ajax_nopriv_get_year', 'get_year_handler');

// ajax handler for getting variants
function get_variant_handler()
{
    $make = isset($_GET['make']) ? sanitize_text_field($_GET['make']) : '';
    $model = isset($_GET['model']) ? sanitize_text_field($_GET['model']) : '';
    $year = isset($_GET['year']) ? sanitize_text_field($_GET['year']) : '';
    // $variants = get_icarasia_variants($make, $model);
    $variants = get_variants_v2($make, $model, $year)['data'];
    wp_send_json_success($variants);
}

add_action('wp_ajax_get_variant', 'get_variant_handler');
add_action('wp_ajax_nopriv_get_variant', 'get_variant_handler');

function get_icarasia_makes()
{
    $make_model_variants = get_icarasia_api_data()['make_model_variants'];
    $car_brands = array_keys($make_model_variants);

    return $car_brands;
}

function get_icarasia_models($make)
{
    $make_model_variant = get_icarasia_api_data()['make_model_variants'];
    $car_models = array_keys($make_model_variant[$make]);

    return $car_models;
}

function get_icarasia_variants($make, $model)
{
    $make_model_variant = get_icarasia_api_data()['make_model_variants'];
    $car_variants = $make_model_variant[$make][$model];

    return $car_variants;
}

function get_icarasia_api_data()
{
    $token = get_token();

    // Fetch the actual data
    $all_listings_api_data_cache_key = 'wapcar_icarasia_api_data';
    $api_data = get_transient($all_listings_api_data_cache_key);
    if ($api_data == false) {
        $page_size = 50;
        $current_page = 1;
        $facets = ['make' => [], 'model' => []];

        $response = wp_remote_get('https://preprod-vehicles.icarasia.com/v2/carlist/en/vehicle', array(
            'method' => 'GET',
            'headers' => array(
                'token' => $token,
                // 'Content-Type' => 'application/json',
            ),
            'query' => array(
                'page_number' => $current_page,
                'page_size' => $page_size
            )
        ));

        if (is_wp_error($response)) {
            return ['success' => false, 'from_cache' => 'no', 'data' => 'Error fetching data'];
        }

        $response = json_decode(wp_remote_retrieve_body($response), true);
        $results = $response['result'];
        $count = $response['count'];
        $pages = ceil($count / ($page_size));

        while ($current_page <= $pages) {
            error_log('page ' . $current_page);
            $response = wp_remote_get('https://preprod-vehicles.icarasia.com/v2/carlist/en/vehicle', array(
                'method' => 'GET',
                'headers' => array(
                    'token' => $token,
                    // 'Content-Type' => 'application/json',
                ),
                'query' => array(
                    'page_number' => $current_page,
                    'page_size' => $page_size
                )
            ));

            if (is_wp_error($response)) {
                return ['success' => false, 'from_cache' => 'no', 'data' => 'Error fetching data'];
            }

            $response = json_decode(wp_remote_retrieve_body($response), true);
            $results = array_merge($results, $response['result']);
            $current_page++;
        }

        $api_data = [
            'count' => $count,
            'data' => $results,
            'facets' => $facets,
            'result' => $results
        ];

        set_transient($all_listings_api_data_cache_key, $api_data, DAY_IN_SECONDS);
    }

    $results = $api_data['result'];
    $make_model_variants = get_transient('icarasia_make_model_variants');
    if ($make_model_variants == false) {


        $make_model_variants = [];

        foreach ($results as $result) {
            if (!isset($facets['make'][$result['make']])) {
                $facets['make'][$result['make']] = 0;
            }
            $facets['make'][$result['make']] = $facets['make'][$result['make']] + 1;

            if (!isset($facets['model'][$result['model']])) {
                $facets['model'][$result['model']] = 0;
            }
            $facets['model'][$result['model']] = $facets['model'][$result['model']] + 1;

            if (!isset($make_model_variants[$result['make']][$result['model']])) {
                if (!isset($make_model_variants[$result['make']])) {
                    $make_model_variants[$result['make']] = [];
                }
                $make_model_variants[$result['make']][$result['model']] = [];
            }
            $make_model_variants[$result['make']][$result['model']][] = $result;
        }

        set_transient('icarasia_make_model_variants', $make_model_variants, 60 * 60);
    }

    return [
        'success' => true,
        'from_cache' => 'no',
        'data' => $api_data,
        'make_model_variants' => $make_model_variants
    ];
}

function get_makes_v2()
{
    $token = get_token();
    $makes = get_transient('icarasia_makes');

    if ($makes != false) {
        return [
            'success' => true,
            'from_cache' => 'yes',
            'data' => $makes
        ];
    }

    // $response = wp_remote_get('https://exapipreprod.carlist.my/v2.0/wapcar/en/listing', array(
    //     'method' => 'GET',
    //     'headers' => array(
    //         'token' => $token,
    //         'Content-Type' => 'application/json',
    //     )
    // ));

    // if (is_wp_error($response)) {
    //     return ['success' => false, 'from_cache' => 'no', 'data' => 'Error fetching data'];
    // }

    // $response = json_decode(wp_remote_retrieve_body($response), true);
    // $facets = $response['facets'];
    // $makes = $facets['make'];
    // $makes = array_keys($makes);
    // sort($makes);

    $response = wp_remote_get('https://vehicles.icarasia.com/v2/carlist/en/make', array(
        'method' => 'GET',
        'headers' => array(
            'token' => $token,
            'Content-Type' => 'application/json',
        ),
    ));
    error_log('made request');

    if (is_wp_error($response)) {
        return ['success' => false, 'from_cache' => 'no', 'data' => 'Error fetching data'];
    }

    $response = json_decode(wp_remote_retrieve_body($response), true);
    $makes = array_map(function ($make) {
        return $make['name'];
    }, $response);

    set_transient('icarasia_makes', $makes, DAY_IN_SECONDS);

    return [
        'success' => true,
        'from_cache' => 'no',
        'data' => $makes
    ];
}

function get_models_v2($make)
{
    $token = get_token();
    $models = get_transient('icarasia_models_' . $make);

    if ($models != false) {
        return [
            'success' => true,
            'from_cache' => 'yes',
            'data' => $models
        ];
    }

    // $response = wp_remote_get('https://exapipreprod.carlist.my/v2.0/wapcar/en/listing', array(
    //     'method' => 'GET',
    //     'headers' => array(
    //         'token' => $token,
    //         'Content-Type' => 'application/json',
    //     ),
    //     'query' => array(
    //         'make' => $make
    //     )
    // ));

    // if (is_wp_error($response)) {
    //     return ['success' => false, 'from_cache' => 'no', 'data' => 'Error fetching data'];
    // }

    // $response = json_decode(wp_remote_retrieve_body($response), true);
    // $facets = $response['facets'];
    // $models = $facets['model'];
    // $models = array_keys($models);
    // sort($models);

    // https://preprod-vehicles.icarasia.com/v2/carlist/en/make/Toyota/

    $response = wp_remote_get('https://vehicles.icarasia.com/v2/carlist/en/make/' . $make, array(
        'method' => 'GET',
        'headers' => array(
            'token' => $token,
            'Content-Type' => 'application/json',
        ),
    ));

    if (is_wp_error($response)) {
        return ['success' => false, 'from_cache' => 'no', 'data' => 'Error fetching data'];
    }

    $response = json_decode(wp_remote_retrieve_body($response), true);
    // read value of property "name" from array of objects
    $models = array_map(function ($model) {
        return $model['name'];
    }, $response);

    set_transient('icarasia_models_' . $make, $models, DAY_IN_SECONDS);

    return [
        'success' => true,
        'from_cache' => 'no',
        'data' => $models
    ];
}

function get_years_v2($make, $model)
{
    // return ['2023', '2022', '2021'];
    $token = get_token();
    $years = get_transient('icarasia_years_' . $make . '_' . $model);

    if ($years != false) {
        return [
            'success' => true,
            'from_cache' => 'yes',
            'data' => $years
        ];
    }

    $response = wp_remote_get('https://vehicles.icarasia.com/v2/carlist/en/make/' . $make . '/' . $model, array(
        'method' => 'GET',
        'headers' => array(
            'token' => $token,
            'Content-Type' => 'application/json',
        ),
    ));

    if (is_wp_error($response)) {
        return ['success' => false, 'from_cache' => 'no', 'data' => 'Error fetching data'];
    }

    $response = json_decode(wp_remote_retrieve_body($response), true);
    $years = $response;

    // read year property from array of objects
    $years = array_map(function ($year) {
        return $year['year'];
    }, $years);

    $years = array_unique($years);
    sort($years);

    set_transient('icarasia_years_' . $make . '_' . $model, $years, DAY_IN_SECONDS);

    return [
        'success' => true,
        'from_cache' => 'no',
        'data' => $years
    ];
}

function get_variants_v2($make, $model, $year)
{
    $token = get_token();
    $variants = get_transient('icarasia_variants_' . $make . '_' . $model . '_' . $year);

    if ($variants != false) {
        return [
            'success' => true,
            'from_cache' => 'yes',
            'data' => $variants
        ];
    }

    $response = wp_remote_get('https://vehicles.icarasia.com/v2/carlist/en/make/' . $make . '/' . $model . '/' . $year, array(
        'method' => 'GET',
        'headers' => array(
            'token' => $token,
            'Content-Type' => 'application/json',
        ),
    ));

    if (is_wp_error($response)) {
        return ['success' => false, 'from_cache' => 'no', 'data' => 'Error fetching data'];
    }

    $response = json_decode(wp_remote_retrieve_body($response), true);
    $variants = $response;
    $variants = $response['vehicles'];

    set_transient('icarasia_variants_' . $make . '_' . $model . '_' . $year, $variants, DAY_IN_SECONDS);

    return [
        'success' => true,
        'from_cache' => 'no',
        'data' => $variants
    ];
}

function get_token()
{
    // Fetch the token
    $all_cars_api_auth_token_cache_key = 'wapcar_all_cars_api_auth_token';
    $token_data = get_transient($all_cars_api_auth_token_cache_key);
    if (!$token_data) {
        $response = wp_remote_post('https://ex-api.carlist.my/v3.0/my/en/authentication/token', array(
            'method' => 'POST',
            'headers' => array(
                'Content-Type' => 'application/json',
                'user_key' => WAPCAR_USER_KEY,
                'app_key' => WAPCAR_APP_KEY,
                //                 'platform' => WAPCAR_PLATFORM,
                'user_secret' => WAPCAR_USER_SECRET
            )
        ));

        if (is_wp_error($response)) {
            return 'Error fetching token';
        }

        set_transient($all_cars_api_auth_token_cache_key, json_decode(wp_remote_retrieve_body($response), true), 60 * 60);

        $token_data = json_decode(wp_remote_retrieve_body($response), true);
    }

    $token = $token_data['token'];

    return $token;
}
