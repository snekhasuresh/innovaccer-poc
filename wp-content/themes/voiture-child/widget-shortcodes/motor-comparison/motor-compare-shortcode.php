<?php

function populate_compare_motor()
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

function selects_motor_shortcode()
{
    ob_start();
?>
    <div class="error-message notification warning" style="display: none;">
        <p>Select at least two cars to compare.</p>
    </div>
    <h1 class="comparision-title wa-title-text">เปรียบเทียบรถ</h1>
    <div class="tab-container" style="display: none;">
        <button class="tab-button active" data-tab="overview">Overview</button>
        <button class="tab-button" data-tab="specs">Specs</button>
    </div>

    <div class="selects-cars-wrapper">
        <?php for ($i = 0; $i < 4; $i++) { ?>
            <div class="selects-car">
                <div class="add-car">
                    <span class="plus-sign">+</span>
                </div>
                <div class="selected-car-details" style="text-align: center;">
                    <img class="variant-image" src="" alt="Variant Image" style="display: none; width: 200px; height: 120px; object-fit: cover; margin: 0 auto;">
                    <p class="variant-name"></p>
                    <p class="variant-price"></p>
                    <div class="switch-btn-container">
                        <button class="switch-button" style="display: none;">สวิตซ์</button>
                    </div>
                </div>

                <p class="select-car-text">เลือกรถยนต์</p>
                <?php populate_compare_motor(); ?>
            </div>
        <?php } ?>
    </div>
    <!-- <button class="compare-button">Compare</button> -->
    <!-- <p class="error-message" style="color: red; display: none;">Select at least two cars to compare.</p> -->

    <!-- Overview Tab Content -->
    <div class="tab-content" id="overview-content">
        <div class="comparison-table-wrapper">
            <table class="comparison-table" style="width: 100%; border-collapse: collapse;">
                <h4 class="specs-head" style="display: none;">Specs Comparison</h4>
                <!-- <thead>
                    <tr>
                        <th>Specs Comparison</th>
                    </tr>
                </thead> -->
                <!-- <thead>
                    <tr>
                        <th>Body Type</th>
                        <th>Segment</th>
                        <th>Transmission</th>
                        <th>Battery Capacity(kWh)</th>
                        <th>Horsepower(PS)</th>
                        <th>Torque (Nm)</th>
                        <th>0-100 km/h (s)</th>
                    </tr>
                </thead> -->
                <tbody>
                </tbody>
            </table>
        </div>
    </div>

    <!-- Specs Tab Content -->
    <div class="tab-content" id="specs-content">
        <!-- <div class="spec-sidebar">
            <ul class="spec-menu">
                <li data-spec="engine">Engine</li>
                <li data-spec="transmission">Transmission</li>
                <li data-spec="performance">Performance</li>
            </ul>
        </div> -->
        <div class="spec-details">
            <table class="spec-table" style="width: 100%; border-collapse: collapse;">
                <thead>
                    <!-- <tr>
                        <th>Specification</th>
                        <th>Car 1</th>
                        <th>Car 2</th>
                        <th>Car 3</th>
                        <th>Car 4</th>
                    </tr> -->
                </thead>
                <tbody>
                    <!-- Specification rows will be injected here -->
                </tbody>
            </table>
            <!-- <h3>Variant Specifications</h3> -->
            <!-- <div class="spec-section" id="price">
                <h4>Price</h4>
                <p>Retail Price</p>
            </div>
            <div class="spec-section" id="cost">
                <h4>Cost</h4>
                <p>Insurance</p>
                <p>Road Tax</p>
                <p>Monthly Payment</p>
            </div>
            <div class="spec-section" id="overview">
                <h4>Overview</h4>
                <p>Brand</p>
                <p>Body Type</p>
                <p>Segment</p>
                <p>Fuel Type</p>
                <p>Model</p>
                <p>Launched Year</p>
                <p>Horse Power(ps)</p>
                <p>Torque(Nm)</p>
                <p>Engine</p>
                <p>Engine Power(PS)</p>
                <p>Electric Engine(PS)</p>
                <p>Length*Width*Heigh(mm)</p>
                <p>0-100 km/h (s)</p>
                <p>Manufacturers Claim(L/100km)</p>
                <p>As Tested(L/100km)</p>
                <p>On Sale</p>
                <p>Warranty Manufacturer</p>
                <p>Top Speed (km/h)</p>
            </div>
            <div class="spec-section" id="dimensions">
                <h4>Dimensions</h4>
                <p>Length(mm)</p>
                <p>Width(mm)</p>
                <p>Height(mm)</p>
                <p>Wheelbase(mm)</p>
                <p>Weight(kg)</p>
                <p>Ground Clearance</p>
                <p>Doors</p>
                <p>Seats</p>
                <p>Fueltank(litres)</p>
                <p>Boot Volume(L)</p>
            </div> -->
        </div>
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
                background-color: #FFB400;
                transition: width 0.3s ease;
                border-radius: 10px;
            }

            /* .tab-button:hover::after {
    width: 100%;
} */


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
    </div>



    <script>
        document.addEventListener('DOMContentLoaded', function() {
            let selectedVariants = [];
            let currentBrandId = '';
            let selectedModels = [];
            let model_name = '';
            let isSwitching = false;


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
                            xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_motor_compare_models&brand_id=' + brandId, true);
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
                                    updateBreadcrumb_motor();
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
                            xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_motor_compare_variants&model_id=' + modelId, true);
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
                                        updateBreadcrumb_motor();
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
                            xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_motor_compare_details&variant_id=' + variantId + '&model_id=' + modelId, true);
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
                                        updateBreadcrumb_motor();


                                        // In your main logic where you create the cancel icon
                                        cancelIcon.addEventListener('click', function() {
                                            console.log('variant cancelled', variantName);
                                            const variantIndex = selectedVariants.findIndex(variant => variant.name === variantName);
                                            cancel_motor_Variant(variantIndex); // Call the reusable function
                                        });
                                        update_motor_Comparison();
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
                                xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_motor_compare_details&variant_id=' + newVariantId + '&model_id=' + newModelId, true);
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
                                            update_motor_Comparison();


                                            updateBreadcrumb_motor();
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
                    updateBreadcrumb_motor();
                });




                function cancel_motor_Variant(variantIndex) {
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
                                cancel_motor_Variant(nextVariantIndex); // Call the same function with the correct index
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
                            update_motor_Comparison();
                        }
                    }
                }


                function update_motor_Comparison() {
                    const selectedCars = selectedVariants.filter(Boolean);
                    // const comparisonTable = document.querySelector('.comparison-table');
                    const comparisonTableBody = document.querySelector('.comparison-table tbody');
                    const tabContainer = document.querySelector('.tab-container');
                    const specDetails = document.querySelector('.spec-details');
                    const comparisionTitle = document.querySelector('.comparision-title');
                    const specTableBody = document.querySelector('.spec-table tbody');
                    // const compareFButton = document.querySelector('.compare-button');
                    const specsHead = document.querySelector('.specs-head');
                    console.log('selectedCars', selectedCars);


                    if (selectedCars.length < 2) {
                        document.querySelector('.error-message').style.display = 'block';
                        setTimeout(() => {
                            document.querySelector('.error-message').style.display = 'none';
                        }, 3000);
                        tabContainer.style.display = 'none';
                        specsHead.style.display = 'none';
                        comparisionTitle.style.display = 'none';
                        // Clear the comparison table and specs table
                        comparisonTableBody.innerHTML = '';
                        specTableBody.innerHTML = '';
                    } else {
                        document.querySelector('.error-message').style.display = 'none';
                        // compareButton.style.display = 'none';
                        // comparisonTableBody.style.display = 'block';
                        // specTableBody.style.display = 'block';
                        specsHead.style.display = 'block';
                        comparisionTitle.style.display = 'block';
                        // comparisonTable.style.display = 'block';
                        // Check if the extra div already exists
                        if (!document.querySelector('.extra-div')) {
                            const extraDiv = document.createElement('div');
                            extraDiv.classList.add('extra-div');
                            extraDiv.innerHTML = `<h4>Variants List</h4>`;


                            // Insert the extra div at the start of the selects-cars-wrapper
                            const wrapper = document.querySelector('.selects-cars-wrapper');
                            wrapper.insertBefore(extraDiv, wrapper.firstChild);
                        }




                        tabContainer.style.display = 'block'; // Hide the table


                        comparisionTitle.textContent = selectedModels.join(' vs ');


                        comparisonTableBody.innerHTML = '';


                        const overview_specs = [{
                                label: 'Body Type',
                                key: 'body_type', // Top-level property in selectedCars[i]
                                source: 'car' // Specifies this is top-level in the selectedCars array
                            },
                            {
                                label: 'Segment',
                                key: 'listing-segment', // From listing_data
                                source: 'listing_data'
                            },
                            {
                                label: 'Transmission',
                                key: 'transmission', // From variant_data
                                source: 'variant_data'
                            },
                            {
                                label: 'Battery Capacity(kWh)',
                                key: 'battery_capacity', // From variant_data
                                source: 'variant_data'
                            },
                            {
                                label: 'Horsepower(PS)',
                                key: 'horsepower', // From variant_data
                                source: 'variant_data'
                            },
                            {
                                label: 'Torque (Nm)',
                                key: 'torque', // From variant_data
                                source: 'variant_data'
                            },
                            {
                                label: '0-100 km/h (s)',
                                key: '0-100_kmph', // From variant_data
                                source: 'variant_data'
                            }
                        ];
                        console.log('overview_specs', overview_specs);
                        overview_specs.forEach(spec => {
                            // Create a row for each spec
                            const row = document.createElement('tr');


                            // First cell (static label)
                            const labelCell = document.createElement('td');
                            labelCell.textContent = spec.label;
                            row.appendChild(labelCell);


                            // Dynamic columns (up to 4 cars)
                            for (let i = 0; i < 4; i++) {
                                const dataCell = document.createElement('td');
                                if (selectedCars[i]) {
                                    let value = '--'; // Default value if data is missing


                                    // Fetching data based on the source
                                    if (spec.source === 'car') {
                                        // Top-level data like body_type
                                        value = selectedCars[i][spec.key] || '--';
                                    } else if (spec.source === 'listing_data') {
                                        // Data from listing_data
                                        value = selectedCars[i].listing_data?.[spec.key] || '--';
                                    } else if (spec.source === 'variant_data') {
                                        // Data from variant_data
                                        value = selectedCars[i].variant_data?.[spec.key] || '--';
                                    }


                                    dataCell.textContent = value;
                                } else {
                                    dataCell.textContent = '--'; // Fallback if no car is selected
                                }
                                console.log('dataCell', dataCell);


                                row.appendChild(dataCell);
                            }


                            // Append the row to the table body
                            comparisonTableBody.appendChild(row);
                        });


                        const specKeys = new Set();
                        selectedCars.forEach(car => {
                            Object.keys(car.variant_data).forEach(key => {
                                if (!key.startsWith('_')) { // Ignore keys that start with '_'
                                    specKeys.add(key);
                                }
                            });
                        });


                        const specs = Array.from(specKeys).sort();
                        specTableBody.innerHTML = '';


                        const numSelectedCars = selectedCars.length;


                        // Add rows for each specification
                        specs.forEach(specKey => {
                            const row = document.createElement('tr');


                            // Create the label cell
                            const specCell = document.createElement('td');
                            specCell.textContent = specKey.replace(/_/g, ' ').replace(/\b\w/g, char => char.toUpperCase()); // Example label transformation
                            row.appendChild(specCell);


                            // Add data cells, ensuring there are always 5 columns
                            for (let i = 0; i < 4; i++) {
                                const dataCell = document.createElement('td');
                                if (i < numSelectedCars) {
                                    const value = selectedCars[i].variant_data[specKey] ? selectedCars[i].variant_data[specKey][0] : '--'; // Use the first value or '--'
                                    dataCell.textContent = value;
                                } else {
                                    dataCell.textContent = '--'; // Placeholder for missing car data
                                }
                                row.appendChild(dataCell);
                            }


                            // Append the row to the table body
                            specTableBody.appendChild(row);
                        });


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
                        xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_motor_compare_models&brand_id=' + currentBrandId, true);
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
                                updateBreadcrumb_motor();
                            }
                        };
                        xhr.send();
                    }
                });


                function loadbrands() {
                    const xhr = new XMLHttpRequest();
                    xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_motor_compare_brands', true);
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
                            updateBreadcrumb_motor();
                        }
                    };
                    xhr.send();
                }

                function updateBreadcrumb_motor() {
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
            const tabButtons = document.querySelectorAll('.tab-button');
            const tabContents = document.querySelectorAll('.tab-content');


            tabButtons.forEach(button => {
                button.addEventListener('click', function() {
                    tabButtons.forEach(btn => btn.classList.remove('active'));
                    button.classList.add('active');


                    const tab = button.dataset.tab;
                    tabContents.forEach(content => content.style.display = 'none');
                    document.getElementById(tab + '-content').style.display = 'block';
                });
            });


            // Initially display the overview tab
            document.querySelector('.tab-button[data-tab="overview"]').click();
        });
    </script>



<?php
    return ob_get_clean();
}

// Shortcode for displaying the car selection
add_shortcode('compare_motor_variant', 'selects_motor_shortcode');


// Register AJAX action for logged-in users
add_action('wp_ajax_get_motor_compare_brands', 'get_motor_compare_brands');

// Register AJAX action for non-logged-in users (if needed)
add_action('wp_ajax_nopriv_get_motor_compare_brands', 'get_motor_compare_brands');

function get_motor_compare_brands()
{
    global $wpdb;

    // Fetch car brands from the database
    $car_brands = $wpdb->get_results("
        SELECT t.term_id, t.name
        FROM {$wpdb->terms} t
        INNER JOIN {$wpdb->term_taxonomy} tt ON t.term_id = tt.term_id
        WHERE tt.taxonomy = 'listing_make' AND tt.parent = 0
        ORDER BY t.name
    ");

    // Prepare the response
    $response = array();
    foreach ($car_brands as $brand) {
        $response[] = array(
            'term_id' => $brand->term_id,
            'name'    => $brand->name
        );
    }

    // Send the JSON response
    wp_send_json($response);
}

// AJAX handler for getting models
add_action('wp_ajax_get_motor_compare_models', 'get_motor_compare_models');
add_action('wp_ajax_nopriv_get_motor_compare_models', 'get_motor_compare_models');

function get_motor_compare_models()
{
    global $wpdb;

    $brand_id = intval($_GET['brand_id']);

    $car_models = $wpdb->get_results($wpdb->prepare("
        SELECT p.ID, p.post_title
        FROM {$wpdb->posts} p
        INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id
        WHERE pm.meta_key = '_listing_make'
        AND pm.meta_value = %d
        ORDER BY p.post_title
    ", $brand_id));

    $response = [];
    foreach ($car_models as $model) {
        $response[] = [
            'ID' => $model->ID,
            'post_title' => $model->post_title,
        ];
    }
    echo json_encode($response);
    wp_die();
}

add_action('wp_ajax_get_motor_compare_variants', 'get_motor_compare_variants');
add_action('wp_ajax_nopriv_get_motor_compare_variants', 'get_motor_compare_variants');


function get_motor_compare_variants()
{
    global $wpdb;

    $model_id = intval($_GET['model_id']);

    // Fetch variants
    $car_variants = $wpdb->get_results($wpdb->prepare("
    SELECT p.ID, p.post_title
    FROM {$wpdb->posts} p
    INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id
    WHERE pm.meta_key = 'model' AND p.post_type = 'variant' AND pm.meta_value LIKE %s
    ORDER BY p.post_title
    ", '%' . $wpdb->esc_like($model_id) . '%'));

    // Check if results are found
    if (empty($car_variants)) {
        error_log('No variants found for model ID: ' . $model_id);
        wp_send_json_error('No variants found');
        wp_die();
    }

    // Send JSON response
    $response = [];
    foreach ($car_variants as $variant) {
        $response[] = [
            'ID' => $variant->ID,
            'post_title' => $variant->post_title,
            'modelId' => $model_id,
        ];
    }
    error_log('Variants response: ' . print_r($response, true));
    wp_send_json_success($response);
    wp_die();
}

add_action('wp_ajax_get_motor_compare_details', 'get_motor_compare_details');
add_action('wp_ajax_nopriv_get_motor_compare_details', 'get_motor_compare_details');


function get_motor_compare_details()
{
    $variant_id = intval($_GET['variant_id']);
    $model_id = intval($_GET['model_id']);
    $variant_post = get_post($variant_id);
    $variant_meta = get_post_meta($variant_id);
    // $image_id = get_post_meta($variant_id, 'image', true);
    $body_type_id = get_post_meta($model_id, '_listing_type', true);
    // print_r($body_type_id);

    $body_type_term = get_term($body_type_id);
    // print_r($body_type_term);

    $body_type_name = is_wp_error($body_type_term) ? '--' : $body_type_term->name;
    // print_r($body_type_name);

    $listing_data = get_post_meta($model_id);
    // Get the image URL and GUID
    //   $image_url = $image_id ? wp_get_attachment_url($image_id) : '';
    //   $image_post = get_post($image_id);
    //   $image_guid = $image_post ? $image_post->guid : '';
    // print_r($listing_data);
    $post_thumbnail_id = get_post_thumbnail_id($variant_id);
    $thumbnail_post = get_post($post_thumbnail_id);
    $guid = $thumbnail_post->guid;
    // $image_url = $image_id ? wp_get_attachment_url($image_id) : '';
    if ($variant_post) {
        $response = [
            'success' => true,
            'data' => [
                'variant_data' =>  $variant_meta,

                'name' => $variant_post->post_title,
                'image_url' => $guid,
                'body_type' => $body_type_name ?? '--',
                // 'segment' => $segment ?? '--',
                'listing_data' => $listing_data,
            ]
        ];
        wp_send_json_success($response);
    } else {
        wp_send_json_error('Variant not found');
    }

    wp_die();
}
?>