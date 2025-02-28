<?php

function add_car_shortcode()
{
    ob_start();
    $token = $_COOKIE["wapcar_token"];

    if (empty($token)) {
        $token = '';
    }
    $response = validate_jwt_token($token);
    if ($response != false) {
        include_once(ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/common/user-menu/user-menu.php');

        echo do_shortcode('[car_popup]');
        return ob_get_clean();
    }

    return ob_get_clean();
}
add_shortcode('add_car', 'add_car_shortcode');

function car_popup_shortcode()
{
    // Enqueue necessary styles and scripts
    wp_enqueue_style('car-popup-css', get_stylesheet_directory_uri() . '/widget-shortcodes/common/car-popup/car-popup.css');
    wp_enqueue_script('car-popup-js', get_stylesheet_directory_uri() . '/widget-shortcodes/common/car-popup/car-popup.js');
    // echo '<button class="add-car-button">Add Car</button>';

    ob_start();
?>
    <div class="add-car-container add-car-button" id="add-car-button">
        <svg class="car-icon" viewBox="0 0 1000 500" xmlns="http://www.w3.org/2000/svg">
            <path d="M800,260 C800,230 780,200 740,200 L660,200 L580,120 C560,100 530,90 500,90 L300,90 C270,90 240,100 220,120 L140,200 L60,200 C20,200 0,230 0,260 L0,340 C0,370 20,400 60,400 L100,400 C100,450 140,490 190,490 C240,490 280,450 280,400 L520,400 C520,450 560,490 610,490 C660,490 700,450 700,400 L740,400 C780,400 800,370 800,340 L800,260 Z" fill="currentColor" />
        </svg>
        <span class="add-car-text">Add My Car</span>
    </div>


    <div class="car-popup-overlay" id="carPopup">
        <div class="car-popup-content">
            <div class="car-popup-header">
                <h2>Add My Car</h2>
                <button class="close-popup">&times;</button>
            </div>
            <div class="car-popup-body">
                <p class="popup-subtitle">Complete The Owner Certification, Get Exclusive Benefits</p>

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
                        <img class="variant-image" src="" alt="Variant Image" style="display: none; width: 200px; height: 120px; object-fit: cover; margin: 0 auto;">
                        <p class="variant-name"></p>
                        <p class="variant-price"></p>
                        <div class="switch-btn-container">
                            <button class="switch-button" style="display: none;">Switch</button>
                        </div>
                    </div>

                    <p class="select-car-text">Select Car</p>
                    <?php populate_compare_cars(); ?>
                </div>

                <div class="license-plate-container">
                    <label for="licensePlate">License Plate <span class="required">*</span></label>
                    <input
                        type="text"
                        id="number-plate"
                        name="licensePlate"
                        placeholder="Enter Your License Plate"
                        required>
                </div>

                <button class="confirm-button" id="confirm-car-button">Confirm</button>
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

            // add event listener on add car button
            document.getElementById('confirm-car-button').addEventListener('click', function(event) {
                console.log('event: ', event);
                event.preventDefault();
                // get number plate and selected variant
                const numberPlate = document.getElementById('number-plate').value;
                const variantName = document.querySelector('.variant-name').innerText;
                const variantItemElement = document.querySelector('.variant-item');
                const variantId = variantItemElement.dataset.variantId; // Read the variantId

                if (numberPlate && variantId) {
                    // make an ajax call to add the car
                    jQuery.ajax({
                        url: '<?php echo admin_url('admin-ajax.php'); ?>',
                        type: 'POST',
                        data: {
                            action: 'add_car',
                            numberPlate: numberPlate,
                            variantId: variantId
                        },
                        success: function(response) {
                            if (response.success) {
                                // refresh page
                                window.location.reload();
                            }
                        },
                        error: function(error) {
                            console.error('Error adding car:', error);
                        }
                    }).done(function(response) {
                        if (response.success) {
                            console.log('Car added successfully');
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


                            console.log('model item', item);
                            const modelId = item.dataset.modelId;
                            currentModelId = modelId;
                            console.log('Model ID for variants:', modelId);
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
                                    console.log('response', response);
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


                                        const cancelIcon = document.createElement('span');
                                        cancelIcon.textContent = 'X'; // You can use an icon or character for this
                                        cancelIcon.classList.add('cancel-icon');
                                        selectedCarDetails.appendChild(cancelIcon);
                                        if (!isSwitching) {
                                            // If it's not a switch event, push the new variant into the selectedVariants array
                                            selectedVariants.push(response.data.data);
                                            console.log('Added new variant to selectedVariants:', response.data.data);
                                        } else {
                                            // If it's a switch event, replace the variant in selectedVariants at the current index
                                            selectedVariants[currentCarIndex] = response.data.data;
                                            console.log('Replaced variant at index', currentCarIndex, 'with', response.data.data);
                                        }
                                        // selectedVariants.push(response.data.data);
                                        updateBreadcrumb();


                                        // In your main logic where you create the cancel icon
                                        cancelIcon.addEventListener('click', function() {
                                            console.log('variant cancelled', variantName);
                                            const variantIndex = selectedVariants.findIndex(variant => variant.name === variantName);
                                            cancelVariant(variantIndex); // Call the reusable function
                                        });
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
                                console.log(newVariantId, newModelId);
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


                                            console.log('selectedVariants after replacement', selectedVariants);


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
add_shortcode('car_popup', 'car_popup_shortcode');

function add_car_handler()
{
    $token = $_COOKIE["wapcar_token"];
    if (empty($token)) {
        $token = '';
    }
    $response = validate_jwt_token($token);
    if ($response != false) {
        $user_id = $response['user_id'];
        if (isset($_POST['numberPlate']) && isset($_POST['variantId'])) {
            $numberPlate = sanitize_text_field($_POST['numberPlate']);
            $variantId = sanitize_text_field($_POST['variantId']);

            // add array(variantId: $variantId, numberPlate: $numberPlate) to user meta if not exist;
            // both variantId and numberPlate is required and are unique
            $user_cars = get_user_meta($user_id, 'user_cars', true);
            if (is_array($user_cars)) {
                $user_cars = array_filter($user_cars, function ($car) use ($variantId, $numberPlate) {
                    return $car['variantId'] !== $variantId || $car['numberPlate'] !== $numberPlate;
                });
            } else {
                $user_cars = [];
            }
            $user_cars[] = array('variantId' => $variantId, 'numberPlate' => $numberPlate);
            update_user_meta($user_id, 'user_cars', $user_cars);

            wp_send_json_success(['message' => 'Car added successfully.', 'variantId' => $variantId, 'numberPlate' => $numberPlate, 'user_id' => $user_id]);
        }
        wp_send_json_error(['message' => 'Invalid request.']);
    }
    wp_send_json_error(['message' => 'Invalid token.']);
}

add_action('wp_ajax_add_car', 'add_car_handler');
add_action('wp_ajax_nopriv_add_car', 'add_car_handler');
