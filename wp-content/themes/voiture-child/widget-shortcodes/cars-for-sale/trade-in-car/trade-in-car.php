<?php

add_shortcode('trade_in_car', 'trade_in_car_shortcode');
function trade_in_car_shortcode()
{
    // Enqueue necessary styles and scripts
    wp_enqueue_style('font-awesome', 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css');
    wp_enqueue_style('car-valuation-css', get_stylesheet_directory_uri() . '/widget-shortcodes/cars-for-sale/trade-in-car/trade-in-car.css');
    wp_enqueue_script('car-valuation-js', get_stylesheet_directory_uri() . '/widget-shortcodes/cars-for-sale/trade-in-car/trade-in-car.js', array(), null, true);

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

    $odometer_ranges = array(
        '0-50000km',
        '50000-100000km',
        '100000-150000km',
        '150000-200000km',
        '200000-250000km',
        '250000-300000km',
        '300000-350000km',
        '350000km and above',
    );

    ob_start();
?>

    <div class="select-cars-container" id="select-cars-container1" style="display: flex; justify-content: space-between;">
        <div class="selects-car" id="selects-car1" data-position="left">
            <div class="add-car" id="add-car1">
                <span class="plus-sign" id="plus-sign1">+</span>
            </div>
            <div class="selected-car-details" style="text-align: center;" id="selected-car-details1">
                <img class="variant-image" id="variant-image1" src="" alt="Variant Image" style="display: none; width: 200px; height: 120px; object-fit: cover; margin: 0 auto;">
                <p class="variant-name" id="variant-name1"></p>
                <p class="variant-price" id="variant-price1"></p>
                <div class="switch-btn-container" id="switch-btn-container1">
                    <button class="switch-button" id="switch-button1" style="display: none;">Switch</button>
                </div>
            </div>

            <p class="select-car-text" id="select-car-text1">Select Your Driven Car</p>
            <?php show_brands_in_dropdown(); ?>
        </div>

        <div class="upgrade-section">
            <i class="fas fa-exchange-alt upgrade-icon"></i>
            <div class="upgrade-text">Upgrade</div>
        </div>

        <div class="selects-car" id="selects-car2" data-position="right">
            <div class="add-car" id="add-car2">
                <span class="plus-sign" id="plus-sign2">+</span>
            </div>
            <div class="selected-car-details" id="selected-car-details2" style="text-align: center;">
                <img class="variant-image" id="variant-image2" src="" alt="Variant Image" style="display: none; width: 200px; height: 120px; object-fit: cover; margin: 0 auto;">
                <p class="variant-name" id="variant-name2"></p>
                <p class="variant-price" id="variant-price2"></p>
                <div class="switch-btn-container" id="switch-btn-container2">
                    <button class="switch-button" id="switch-button2" style="display: none;">Switch</button>
                </div>
            </div>

            <p class="select-car-text" id="select-car-text2">Select Your Dream Car</p>
            <?php show_brands_in_dropdown(); ?>
        </div>
    </div>

    <div>
        <!-- <button class="trade-in-button">Trade In</button> -->
        <button class="sell-my-car-button" onclick="window.location.href='https://www.carsome.my/sell-car';">Sell My Car</button>
    </div>


    <div class="car-popup-overlay" id="carPopup">
        <div class="car-popup-content">
            <div class="car-popup-header">
                <h2>Enter Your Driven Car Details</h2>
                <button class="close-popup">&times;</button>
            </div>

            <div class="car-popup-body">
                <!-- odometer dropdown -->
                <div class="odometer-container">
                    <label for="odometer">Odometer <span class="required">*</span></label>
                    <select id="odometer" name="odometer" required>
                        <option value="">Select Odometer</option>
                        <?php
                        foreach ($odometer_ranges as $range) {
                            echo '<option value="' . $range . '">' . $range . '</option>';
                        }
                        ?>
                    </select>
                </div>

                <!-- Insurance Expiry Month - Drpdown of months -->
                <div class="insurance-expiry-container">
                    <label for="insuranceExpiry">Insurance Expiry <span class="required">*</span></label>
                    <select id="insurance-expiry" name="insuranceExpiry" required>
                        <option value="">Select Month</option>
                        <?php
                        foreach ($months as $month) {
                            echo '<option value="' . $month . '">' . $month . '</option>';
                        }
                        ?>
                    </select>
                </div>

                <button class="confirm-button" id="confirm-car-button">
                    <a href="https://www.carsome.my/sell-car">Sell My Car</a>
                </button>
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


            // Add click event listener for confirm-button
            document.querySelector('.confirm-button').addEventListener('click', function() {
                const variantName1 = document.getElementById('variant-name1').textContent;
                const variantName2 = document.getElementById('variant-name2').textContent;
                variantData = [];
                for (let i = 0; i < selectedVariants.length; i++) {
                    const variant = selectedVariants[i];
                    const variantId = variant.id;
                    const variantName = variant.name;

                    if (variantName == variantName1) {
                        variantData.push({
                            position: 1,
                            id: variantId,
                            name: variantName,
                        })
                    } else if (variantName == variantName2) {
                        variantData.push({
                            position: 2,
                            id: variantId,
                            name: variantName,
                        })
                    }
                }

                // make an API call to get the quote
                // get_quote_from_api();
            })

            document.querySelectorAll('.selects-car').forEach(function(container) {
                const addCarButton = container.querySelector('.plus-sign');
                const addCar = container.querySelector('.add-car');
                const dropdown = container.querySelector('.dropdown-menu');
                const breadcrumbElement = dropdown.querySelector('.breadcrumb');
                const selectCarText = container.querySelector('.select-car-text');
                const switchButton = container.querySelector('.switch-button');


                const selectedCarDetails = container.querySelector('.selected-car-details');
                const variantImage = selectedCarDetails.querySelector('.variant-image');
                const variantNameElem = selectedCarDetails.querySelector('.variant-name');
                const variantPriceElem = selectedCarDetails.querySelector('.variant-price');


                let selectedBrand = '';
                let selectedModel = '';
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
                            // const loader = document.createElement('div');
                            // loader.className = 'loader';
                            // loader.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
                            dropdown.appendChild(loader);


                            const brandId = item.dataset.brandId;
                            currentBrandId = brandId;
                            const xhr = new XMLHttpRequest();
                            xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_compare_models&brand_id=' + brandId, true);
                            xhr.onload = function() {
                                if (xhr.status === 200) {
                                    const response = JSON.parse(xhr.responseText);


                                    dropdown.innerHTML = '';
                                    dropdown.appendChild(breadcrumbElement);


                                    response.forEach(function(model) {
                                        const modelItem = document.createElement('div');
                                        modelItem.className = 'dropdown-item model-item';
                                        modelItem.dataset.modelId = model.ID;
                                        modelItem.innerHTML = '<span class="model-name">' + model.post_title + '</span>';
                                        dropdown.appendChild(modelItem);
                                    });


                                    loader.remove();
                                    selectedBrand = 'Model';
                                    updateBreadcrumb();
                                }
                            };
                            xhr.send();
                        } else if (item.classList.contains('model-item')) {
                            const selectedModelName = event.target.querySelector('.model-name').textContent;


                            if (!selectedModels.includes(selectedModelName)) {
                                selectedModels.push(selectedModelName);
                            }


                            dropdown.appendChild(loader);


                            const modelId = item.dataset.modelId;
                            currentModelId = modelId;
                            const xhr = new XMLHttpRequest();
                            xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_compare_variants&model_id=' + modelId, true);
                            xhr.onload = function() {
                                if (xhr.status === 200) {
                                    const response = JSON.parse(xhr.responseText);


                                    if (response.success) {
                                        // Clear dropdown and show breadcrumb
                                        dropdown.innerHTML = '';
                                        dropdown.appendChild(breadcrumbElement);


                                        // Loop through the returned variants
                                        response.data.forEach(function(variant) {
                                            const variantItem = document.createElement('div');
                                            variantItem.className = 'dropdown-item variant-item';
                                            variantItem.dataset.variantId = variant.ID;
                                            variantItem.dataset.variantName = variant.name;
                                            variantItem.dataset.modelId = variant.modelId;
                                            variantItem.innerHTML = '<span class="variant-name">' + variant.post_title + '</span>';
                                            dropdown.appendChild(variantItem);
                                        });


                                        // Update breadcrumb with the selected model
                                        selectedModel = 'Variant';
                                        selectedVariant = '';
                                        updateBreadcrumb();
                                    } else {
                                        console.error('No variants found for this model.');
                                    }
                                    loader.remove();
                                }
                            };
                            xhr.send();
                        } else if (item.classList.contains('variant-item')) {
                            const variantName = item.querySelector('.variant-name').textContent;
                            let selectedIndex;
                            const variantId = item.dataset.variantId;
                            const modelId = item.dataset.modelId;
                            // Send an AJAX request to fetch additional variant details (image and price)
                            const xhr = new XMLHttpRequest();
                            xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_variant_details&variant_id=' + variantId + '&model_id=' + modelId, true);
                            xhr.onload = function() {
                                if (xhr.status === 200) {
                                    const response = JSON.parse(xhr.responseText);
                                    if (response.success) {
                                        data = response.data.data
                                        variantImage.src = data.image_url;
                                        variantImage.style.display = 'block'; // Show the image
                                        variantNameElem.textContent = data.name;
                                        variantPriceElem.textContent = 'RM ' + data.variant_data['retail_price'];


                                        // Hide plus-sign and "Select Car"
                                        addCar.style.display = 'none';
                                        selectCarText.style.display = 'none';
                                        // Show switch button
                                        switchButton.style.display = 'block';

                                        // const cancelIcon = document.createElement('span');
                                        // cancelIcon.textContent = 'X'; // You can use an icon or character for this
                                        // cancelIcon.classList.add('cancel-icon');
                                        // selectedCarDetails.appendChild(cancelIcon);
                                        if (!isSwitching) {
                                            // If it's not a switch event, push the new variant into the selectedVariants array
                                            selectedVariants.push(response.data.data);
                                        } else {
                                            // If it's a switch event, replace the variant in selectedVariants at the current index
                                            selectedVariants[currentCarIndex] = response.data.data;
                                        }
                                        // selectedVariants.push(response.data.data);
                                        updateBreadcrumb();


                                        // In your main logic where you create the cancel icon
                                        // cancelIcon.addEventListener('click', function() {
                                        //     console.log('variant cancelled', variantName);
                                        //     const variantIndex = selectedVariants.findIndex(variant => variant.name === variantName);
                                        //     cancelVariant(variantIndex); // Call the reusable function
                                        // });
                                        // updateComparison();
                                    } else {
                                        console.error('Failed to fetch variant details.');
                                    }
                                }
                            };
                            xhr.send();


                            dropdown.style.display = 'none';
                        }
                    }
                });


                switchButton.addEventListener('click', function() {
                    isSwitching = true;
                    // Clear the dropdown and show the breadcrumb for selecting a new variant
                    dropdown.innerHTML = '';
                    dropdown.appendChild(breadcrumbElement);
                    dropdown.style.display = 'block';
                    loadbrands();
                    // Hide the car details and plus sign
                    addCar.style.display = 'none';
                    selectCarText.style.display = 'none';

                    // Get the variant name to replace
                    const variantNameToReplace = variantNameElem.textContent.trim();

                    // Find the index of the car that needs to be replaced
                    const currentCarIndex = selectedVariants.findIndex(car => car.name === variantNameToReplace);

                    if (currentCarIndex !== -1) {
                        // Ensure we replace the current variant with a new one by fetching the new variant data
                        dropdown.addEventListener('click', function(event) {
                            const newItem = event.target.closest('.dropdown-item');
                            if (newItem && newItem.classList.contains('variant-item')) {
                                const newVariantId = newItem.dataset.variantId;
                                const newModelId = newItem.dataset.modelId;

                                // Fetch new variant details via AJAX
                                const xhr = new XMLHttpRequest();
                                xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_variant_details&variant_id=' + newVariantId + '&model_id=' + newModelId, true);
                                xhr.onload = function() {
                                    if (xhr.status === 200) {
                                        const response = JSON.parse(xhr.responseText);
                                        if (response.success) {
                                            const newVariantData = response.data.data;


                                            // Update the selectedVariants array with the new variant at the same index
                                            selectedVariants[currentCarIndex] = {
                                                variant_data: newVariantData.variant_data,
                                                name: newVariantData.name,
                                                image_url: newVariantData.image_url,
                                                body_type: newVariantData.body_type,
                                                listing_data: newVariantData.listing_data,
                                            };

                                            // Update the UI with new variant details
                                            variantImage.src = newVariantData.image_url;
                                            variantNameElem.textContent = newVariantData.name;
                                            variantPriceElem.textContent = 'RM ' + newVariantData.variant_data['retail_price'];
                                            variantImage.style.display = 'block';


                                            // Update the breadcrumbs and hide dropdown
                                            updateComparison();


                                            updateBreadcrumb();
                                            dropdown.style.display = 'none';
                                        } else {
                                            console.error('Failed to fetch new variant details.');
                                        }
                                    }
                                };
                                xhr.send();
                            }
                        });
                    } else {
                        console.warn('No matching car found to replace. Adding a new variant is not allowed.');
                    }


                    // Reset breadcrumb for a new selection
                    selectedBrand = '';
                    selectedModel = '';
                    selectedVariant = '';
                    updateBreadcrumb();
                });




                function cancelVariant(variantIndex) {
                    if (variantIndex !== -1) {
                        // Remove the variant from the array
                        selectedVariants.splice(variantIndex, 1);
                        // Shift the UI elements up, filling the removed variant's space
                        for (let i = variantIndex; i < selectedVariants.length; i++) {
                            const nextVariant = selectedVariants[i];
                            const nextContainer = document.querySelectorAll('.selects-car')[i];
                            const nextVariantImage = nextContainer.querySelector('.variant-image');
                            const nextVariantNameElem = nextContainer.querySelector('.variant-name');
                            const nextVariantPriceElem = nextContainer.querySelector('.variant-price');
                            const nextSwitchButton = nextContainer.querySelector('.switch-button');


                            nextVariantImage.src = nextVariant.image_url;
                            nextVariantImage.style.display = 'block';
                            nextVariantNameElem.textContent = nextVariant.name;
                            nextVariantPriceElem.textContent = 'RM ' + nextVariant.variant_data['retail_price'];


                            // Update or create cancel icon for the shifted variant
                            let nextCancelIcon = nextContainer.querySelector('.cancel-icon');
                            if (!nextCancelIcon) {
                                nextCancelIcon = document.createElement('span');
                                nextCancelIcon.textContent = 'X';
                                nextCancelIcon.classList.add('cancel-icon');
                                nextContainer.querySelector('.selected-car-details').appendChild(nextCancelIcon);
                            }


                            // Set the correct variant name for the nextCancelIcon
                            nextCancelIcon.dataset.variantName = nextVariant.name; // Store the variant name in a data attribute


                            nextCancelIcon.onclick = function() {
                                const nextVariantIndex = selectedVariants.findIndex(variant => variant.name === this.dataset.variantName);
                                cancelVariant(nextVariantIndex); // Call the same function with the correct index
                            };
                        }


                        // Clear the UI for the last container since it's now empty
                        const lastContainer = document.querySelectorAll('.selects-car')[selectedVariants.length];
                        if (lastContainer) {
                            lastContainer.querySelector('.variant-image').style.display = 'none';
                            lastContainer.querySelector('.variant-name').textContent = '';
                            lastContainer.querySelector('.variant-price').textContent = '';
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
                }

                breadcrumbElement.addEventListener('click', function(event) {
                    const clickedText = event.target.textContent.trim();
                    if (clickedText === 'Brand' && selectedBrand !== '') {
                        dropdown.innerHTML = '';
                        dropdown.appendChild(breadcrumbElement);
                        loadbrands();
                    } else if (clickedText === 'Model' && selectedBrand !== '' && selectedModel !== '') {
                        dropdown.innerHTML = '';
                        dropdown.appendChild(breadcrumbElement);
                        // Fetch and display model dropdown
                        const xhr = new XMLHttpRequest();
                        xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_compare_models&brand_id=' + currentBrandId, true);
                        xhr.onload = function() {
                            if (xhr.status === 200) {
                                const response = JSON.parse(xhr.responseText);
                                response.forEach(function(model) {
                                    const modelItem = document.createElement('div');
                                    modelItem.className = 'dropdown-item model-item';
                                    modelItem.dataset.modelId = model.ID;
                                    modelItem.innerHTML = '<span class="model-name">' + model.post_title + '</span>';
                                    dropdown.appendChild(modelItem);
                                });
                                selectedBrand = 'Model';
                                selectedModel = '';
                                selectedVariant = '';
                                updateBreadcrumb();
                            }
                        };
                        xhr.send();
                    }
                });


                function loadbrands() {
                    const xhr = new XMLHttpRequest();
                    xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_compare_brands', true);
                    xhr.onload = function() {
                        if (xhr.status === 200) {
                            const response = JSON.parse(xhr.responseText);
                            response.forEach(function(brand) {
                                const brandItem = document.createElement('div');
                                brandItem.className = 'dropdown-item brand-item';
                                brandItem.dataset.brandId = brand.term_id;
                                brandItem.innerHTML = '<span class="brand-name">' + brand.name + '</span>';
                                dropdown.appendChild(brandItem);
                            });
                            selectedBrand = '';
                            selectedModel = '';
                            selectedVariant = '';
                            updateBreadcrumb();
                        }
                    };
                    xhr.send();
                }

                function updateBreadcrumb() {
                    let breadcrumb = 'Brand';
                    if (selectedBrand) breadcrumb += ' > ' + selectedBrand;
                    if (selectedModel) breadcrumb += ' > ' + selectedModel;
                    if (selectedVariant) breadcrumb += ' > ' + selectedVariant;
                    breadcrumbElement.innerHTML = breadcrumb
                        .split(' > ')
                        .map((text, index) => `<span class="breadcrumb-item">${text}</span>`)
                        .join(' > ');
                }
            });
        });
    </script>
<?php
    return ob_get_clean();
}

function show_brands_in_dropdown()
{
    global $wpdb;

    $car_brands = $wpdb->get_results("
        SELECT t.term_id, t.name
        FROM {$wpdb->terms} t
        INNER JOIN {$wpdb->term_taxonomy} tt ON t.term_id = tt.term_id
        WHERE tt.taxonomy = 'listing_make' AND tt.parent = 0
        ORDER BY t.name
    ");
    echo '<div class="dropdown-menu">';
    echo '<div class="breadcrumb">Brand</div>';

    foreach ($car_brands as $brand) {
        echo '<div class="dropdown-item brand-item" data-brand-id="' . esc_attr($brand->term_id) . '">';
        echo '<span class="brand-name">' . esc_html($brand->name) . '</span>';
        echo '<div class="sub-menu model-menu" style="display: none;"></div>';
        echo '</div>';
    }
    echo '</div>';
}
