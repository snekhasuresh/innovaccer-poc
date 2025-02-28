<?php
include_once(ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/calculator/template-calculators.php');
include_once(ABSPATH . 'wp-content/themes/voiture-child/widget-shortcodes/car-comparison/car-compare-shortcode.php');

function add_my_car_shortcode()
{
    // import C:\xampp\htdocs\wapcar_prepod_testing2\wp-content\themes\voiture-child\js\tools-calculator.js
    wp_enqueue_script('tools-form-calculator-script', get_stylesheet_directory_uri() . '/js/tools-calculator.js', array('jquery'), null, true);
    ob_start();
?>
    <div class="input-group">
        <span><?php echo do_shortcode('[compare_cars_variant2]'); ?></span>
        <!-- <span><input type="text" class="input-field" /></span> -->
        <!-- <span><input type="submit" class="btn-default" id="add-car-button" /></span> -->
    </div>

<?php
    return ob_get_clean();
}
add_shortcode('add_my_car', 'add_my_car_shortcode');


function selects_cars_shortcode2()
{
    ob_start();
?>
    <!-- <div class="error-message notification warning" style="display: none;">
        <p>Select at least two cars to compare.</p>
    </div> -->
    <!-- <h1 class="comparision-title wa-title-text">Car Compare</h1>
    <div class="tab-container" style="display: none;">
        <button class="tab-button active" data-tab="overview">Overview</button>
        <button class="tab-button" data-tab="specs">Specs</button>
    </div> -->

    <!-- <div >
        <h2>Add My Car</h2>
        <h3>Complete the car owner certification, get exclusive benefits</h3>
    </div> -->

    <div class="selects-cars-wrapper">
        <?php for ($i = 0; $i < 1; $i++) { ?>
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
        <?php } ?>
    </div>
    <div>
        <span><input type="text" class="input-field" id="number-plate" /></span>
        <span><input type="submit" class="btn-default" id="add-car-button" /></span>
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
            document.getElementById('add-car-button').addEventListener('click', function(event) {
                event.preventDefault();
                // get number plate and selected variant
                const numberPlate = document.getElementById('number-plate').value;
                const variantName = document.querySelector('.variant-name').innerText;
                const variantItemElement = document.querySelector('.variant-item');
                const variantId = variantItemElement.dataset.variantId; // Read the variantId

                console.log('numberPlate: ', numberPlate);
                console.log('variantName: ', variantName);
                console.log('Variant ID: ', variantId);

                if (numberPlate && variantName && variantId) {
                    // make an API call to add the car
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
                            console.log('item', item);


                            const variantName = item.querySelector('.variant-name').textContent;
                            console.log('variantName', variantName);
                            let selectedIndex;
                            const variantId = item.dataset.variantId;
                            const modelId = item.dataset.modelId;
                            console.log('modelId', modelId);
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
                                        console.log('new data', data);
                                        // Show switch button
                                        switchButton.style.display = 'block';


                                        console.log('new selectedVariants', selectedVariants);


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


                /*switchButton.addEventListener('click', function() {
                    isSwitching = true;
                    // Clear the dropdown and show the breadcrumb for selecting a new variant
                    dropdown.innerHTML = '';
                    dropdown.appendChild(breadcrumbElement);
                    dropdown.style.display = 'block';
                    loadbrands();
                    // Hide the car details and plus sign
                    addCar.style.display = 'none';
                    selectCarText.style.display = 'none';


                    console.log('selectedVariants before replacement', selectedVariants);


                    // Get the variant name to replace
                    const variantNameToReplace = variantNameElem.textContent.trim();


                    // Find the index of the car that needs to be replaced
                    const currentCarIndex = selectedVariants.findIndex(car => car.name === variantNameToReplace);


                    console.log('currentCarIndex', currentCarIndex);


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
                });*/




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
                    console.log('clickedText', clickedText);
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


            // Tab switching functionality
            // const tabButtons = document.querySelectorAll('.tab-button');
            // const tabContents = document.querySelectorAll('.tab-content');


            // tabButtons.forEach(button => {
            //     button.addEventListener('click', function() {
            //         tabButtons.forEach(btn => btn.classList.remove('active'));
            //         button.classList.add('active');


            //         const tab = button.dataset.tab;
            //         tabContents.forEach(content => content.style.display = 'none');
            //         document.getElementById(tab + '-content').style.display = 'block';
            //     });
            // });


            // // Initially display the overview tab
            // document.querySelector('.tab-button[data-tab="overview"]').click();
        });
    </script>

    <style>
        .tab-container {
            display: flex;
            justify-content: space-around;
            padding: 0;
            border-bottom: 1px solid #e0e0e0;
            /* Optional border under the tab container */
        }

        .variant-name {
            height: 40px;
            font-family: "Roboto";
            font-weight: 700;
            font-size: 14px;
            color: #262626;
            line-height: 20px;
            white-space: unset;
            overflow: hidden;
            text-overflow: ellipsis;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            margin-bottom: 8px;
        }

        .variant-image {
            display: flex;
            justify-content: center;
        }

        .variant-price {
            display: block;
            font-family: "Roboto";
            font-weight: Bold;
            font-size: 14px;
            color: #576b95;
            letter-spacing: 0;
            line-height: 20px;
            margin-bottom: 12px;
        }

        .tab-button {
            background: none;
            border: none;
            font-size: 16px;
            color: #333;
            padding: 10px 20px;
            cursor: pointer;
            position: relative;
            transition: color 0.3s ease;
            font-weight: bold;
        }

        .tab-button:hover {
            color: #f5c34b !important;
            /* Change color on hover */
        }

        .tab-button.active::after {
            content: '';
            position: absolute;
            left: 0;
            bottom: -1px;
            width: 100%;
            height: 5px;
            background-color: #f5c34b !important;
            transition: width 0.3s ease;
            border-radius: 10px;
        }

        .tab-button.active::after {
            content: '';
            position: absolute;
            left: 28px;
            bottom: -1px;
            width: 42%;
            height: 5px;
            background-color: #32D0C6;
            transition: width 0.3s ease;
            border-radius: 10px;
        }

        .selects-cars-wrapper {
            display: flex;
            justify-content: space-around;
            margin-bottom: 20px;
        }

        .selects-car {
            width: 100%;
            height: 250px;
            border: 1px solid #d9d9d9;
            border-radius: 5px;
            text-align: center;
            position: relative;
        }

        .add-car {
            width: 60px;
            height: 60px;
            background-color: #dadce4;
            border-radius: 50%;
            color: white;
            border: 1px dashed darkgray;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            cursor: pointer;
        }

        .plus-sign {
            font-size: 39px;
            color: #262626;
            line-height: 51px;
        }

        .dropdown-menu {
            background-color: white;
            border: 1px solid #ccc;
            border-radius: 5px;
            box-shadow: 0px 8px 16px rgba(0, 0, 0, 0.2);
            position: absolute;
            top: 100%;
            left: 0;
            width: 100%;
            display: none;
            max-height: 400px;
            overflow-y: auto;
        }

        .dropdown-item {
            padding: 10px;
            cursor: pointer;
        }

        .dropdown-item:hover {
            background-color: #f1f1f1;
        }

        .breadcrumb {
            font-weight: bold;
            cursor: pointer;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            display: inline-block;
            width: 100%;
            position: sticky;
            top: 0;
            background-color: white;
            z-index: 1;
            padding: 10px;
            margin-bottom: 0px;
        }

        .loader {
            text-align: center;
            padding: 10px;
            font-size: 14px;
        }

        .switch-btn-container {

            display: flex !important;
            justify-content: center !important;
            padding: 10px;
        }

        .switch-button {
            width: 77%;
            padding: 6px 0;
            margin-top: -20px;

            border: 1px solid #d9d9d9;
            border-radius: 4px;
            cursor: pointer;
            background-color: transparent;
        }

        .extra-div {
            width: 100%;
            height: 250px;
            border: 1px solid #ccc;
            border-radius: 5px;
            text-align: center;
            position: relative;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .extra-div h4 {
            margin-top: 0;
            font-size: 18px;
            font-weight: bold;
        }

        .cancel-icon {
            position: absolute;
            top: 5px;
            right: 5px;
            cursor: pointer;
            font-size: 15px;
            /*     color: red; */
        }

        .comparison-table th,
        .spec-table th,
        .spec-table td,
        .comparison-table td {
            width: 20%;
            /*     text-align: center; */
            border: 1px solid #ccc;
            padding: 10px;
        }

        .comparison-table,
        .spec-table {
            width: 100%;
            table-layout: fixed;
            border-collapse: collapse;
        }

        .notification {
            position: absolute;
            top: 50px;
            right: 20px;
            padding: 10px;
            border-radius: 5px;
            background-color: #fff;
            border: 1px solid #ddd;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.2);
        }

        .warning {
            background-color: #ffe6e6;
            border-color: #ff9999;
        }
    </style>

<?php
    return ob_get_clean();
}

// Shortcode for displaying the car selection
add_shortcode('compare_cars_variant2', 'selects_cars_shortcode2');
