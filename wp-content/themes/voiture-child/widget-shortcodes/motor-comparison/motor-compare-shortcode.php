<?php

function populate_compare_motor()
{
    global $wpdb;

    $car_brands = $wpdb->get_results("
        SELECT t.term_id, t.name
        FROM {$wpdb->terms} t
        INNER JOIN {$wpdb->term_taxonomy} tt ON t.term_id = tt.term_id
        WHERE tt.taxonomy = 'motorcycle_make' AND tt.parent = 0
        ORDER BY t.name
    ");
    echo '<div class="dropdown-menu">';
    echo '<div class="breadcrumb">ยี่ห้อ</div>';

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

    <h1 class="comparision-title wa-title-text">Perbandingan motor</h1>
    <div class="tab-container" style="display: none;">
        <button class="tab-button active" data-tab="overview">Overview</button>
        <button class="tab-button" data-tab="specs">Spesifikasi</button>
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
                        <button class="switch-button" style="display: none;">Beralih</button>
                    </div>
                </div>

                <p class="select-car-text">Pilih Motor</p>
                <?php populate_compare_motor(); ?>
            </div>
        <?php } ?>
    </div>
    <div class="error-message notification warning" style="display: none;">
        <p>Pilih setidaknya dua motor untuk perbandingan</p>
    </div>
    <!-- <button class="compare-button">Compare</button> -->
    <!-- <p class="error-message" style="color: red; display: none;">Select at least two cars to compare.</p> -->

    <!-- Overview Tab Content -->
    <div class="tab-content" id="overview-content">
        <div class="compare-car-name-con">
            <!-- <div class="first-car-name">
                <span>Honda</span>
            </div>
            <div class="second-car-name">
                <span>Audi</span>
            </div> -->
        </div>
        <div class="comparison-table-wrapper">
            <table class="comparison-table" style="width: 100%; border-collapse: collapse;">
                <h4 class="specs-head" style="display: none;">Perbandingan Spek</h4>
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
        <div class="compare-car-name-con">
            <!-- <div class="first-car-name">
                <span>Honda</span>
            </div>
            <div class="second-car-name">
                <span>Audi</span>
            </div> -->
        </div>
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
            .compare-car-name-con {
                display: none;
            }

            @media (max-width: 768px) {
                .comparison-table-wrapper {
                    overflow-x: auto;
                    overflow-y: hidden;
                    max-width: 100%;
                }

                .comparison-table {
                    min-width: 600px;
                    table-layout: auto;
                }

                .comparison-table thead {
                    position: sticky;
                    top: 0;
                    background-color: #f8f8f8;
                    z-index: 1;
                }

                .comparison-table th,
                .comparison-table td {
                    padding: 10px;
                    font-size: 12px;
                    word-wrap: break-word;
                    white-space: normal;
                }

                .comparison-table-wrapper::-webkit-scrollbar {
                    height: 4px;
                }

                .comparison-table-wrapper::-webkit-scrollbar-thumb {
                    background: #ccc;
                    border-radius: 2px;
                }

                .spec-details {
                    overflow-x: auto;
                    overflow-y: hidden;
                    max-width: 100%;
                    border: 1px solid #ddd;
                    margin-bottom: 10px;
                }

                .spec-table {
                    min-width: 600px;
                    width: 100%;
                    border-collapse: collapse;
                    table-layout: auto;
                }

                .spec-table thead {
                    position: sticky;
                    top: 0;
                    background-color: #f8f8f8;
                    z-index: 1;
                }

                .spec-table th,
                .spec-table td {
                    padding: 10px;
                    font-size: 12px;
                    word-wrap: break-word;
                    white-space: normal;
                    text-align: left;
                    border: 1px solid #ddd;
                }

                .spec-table-wrapper::-webkit-scrollbar {
                    height: 6px;
                }

                .spec-table-wrapper::-webkit-scrollbar-thumb {
                    background: #ccc;
                    border-radius: 2px;
                }
            }

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


            .selects-cars-wrapper {
                display: flex;
                justify-content: space-around;
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
                position: relative;
                top: 50px;
                right: 20px;
                padding: 10px;
                border-radius: 5px;
                background-color: #fff;
                border: 1px solid #ddd;
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.2);
                top: 0px !important;
                width: 288px;
                margin-left: 15px;
            }

            .warning {
                background-color: #ffe6e6;
                border-color: #ff9999;
            }

            @media screen and (max-width: 768px) {
                .second-car-name {

                    font-family: 'Roboto';
                    font-weight: 700;
                    font-size: 16px;
                    color: #576b95;
                }

                .first-car-name {
                    margin-left: 72px;
                    font-family: 'Roboto';
                    font-weight: 700;
                    font-size: 16px;
                    color: #576b95;
                }

                .compare-car-name-con {
                    position: sticky !important;
                    top: 25px;
                    background-color: white;
                    z-index: 10;
                    display: flex !important;
                    justify-content: space-around !important;
                    padding: 10px 0;
                    border-bottom: 1px solid #ddd;
                }

                .selects-cars-wrapper {
                    display: grid;
                    margin-bottom: 20px;
                    grid-template-columns: 1fr;
                }

                .tab-container {
                    position: sticky !important;
                    top: 0;
                    background-color: white;
                    z-index: 10;
                    display: flex !important;
                    justify-content: space-around !important;
                    padding: 10px 0;
                    border-bottom: 1px solid #ddd;
                }

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

            get_motors_from_url();

            // First function: Compare cars from URL
            function get_motors_from_url() {
                const currentUrl = window.location.pathname;

                if (currentUrl.includes("-vs-")) {
                    const path = currentUrl.replace("/so-sanh-xe-may", "");
                    const [car1, car2] = path.split("-vs-");

                    if (car1 && car2) {
                        const ajaxData = {
                            action: "compare_motors_action",
                            car1: car1,
                            car2: car2
                        };

                        // Initial AJAX request wrapped in a Promise
                        jQuery.ajax({
                            url: '<?php echo admin_url('admin-ajax.php'); ?>',
                            type: "POST",
                            data: ajaxData,
                            dataType: "json",
                            success: function(response) {
                                if (response.success && response.data.listings) {
                                    // console.log("Comparison Data:", response.data.listings);
                                    let containers = document.querySelectorAll('.selects-car');
                                    // console.log('got containers......');

                                    // Create an array of promises for each compare_cars_from_url call
                                    const promises = response.data.listings.map((listing, index) => {
                                        const modelId = listing.model_id;
                                        const variantId = listing.variant_id;
                                        return compare_motors_from_url(modelId, variantId, containers[index]);
                                    });

                                    // Wait for all promises to resolve
                                    Promise.all(promises)
                                        .then(() => {
                                            // console.log('All variant details fetched successfully');
                                            updateMotorComparison(selectedVariants);
                                        })
                                        .catch(error => {
                                            console.error("Error in Promise.all:", error);
                                        });
                                } else {
                                    console.error("No listings found:", response.data.message);
                                }
                            },
                            error: function(xhr, status, error) {
                                console.error("AJAX Error:", error);
                            }
                        });
                    }
                }
            }

            function generateMotorReviewsJsonLd(listingName, reviews) {
                let totalRating = 0;
                const reviewCount = reviews.length;

                const jsonLd = {
                    "@context": "https://schema.org/",
                    "@type": "Product",
                    "name": listingName,
                    "review": []
                };

                reviews.forEach(review => {
                    totalRating += review.total_score;

                    jsonLd.review.push({
                        "@type": "Review",
                        "author": {
                            "@type": "Person",
                            "name": review.user_name
                        },
                        "datePublished": review.date,
                        "reviewRating": {
                            "@type": "Rating",
                            "ratingValue": review.total_score,
                            "bestRating": "5"
                        },
                        "positiveNotes": review.pros,
                        "negativeNotes": review.cons
                    });
                });

                // Calculate average rating
                if (reviewCount > 0) {
                    const averageRating = totalRating / reviewCount;

                    jsonLd.aggregateRating = {
                        "@type": "AggregateRating",
                        "ratingValue": Number(averageRating.toFixed(1)),
                        "reviewCount": reviewCount,
                        "bestRating": "5"
                    };
                }

                // Create and insert the script element
                const script = document.createElement('script');
                script.type = 'application/ld+json';
                script.text = JSON.stringify(jsonLd);
                document.head.appendChild(script);
            }

            function generateMotorReviewsJsonLd(listingName, reviews) {
                let totalRating = 0;
                const reviewCount = reviews.length;

                const jsonLd = {
                    "@context": "https://schema.org/",
                    "@type": "Product",
                    "name": listingName,
                    "review": []
                };

                reviews.forEach(review => {
                    totalRating += review.total_score;

                    jsonLd.review.push({
                        "@type": "Review",
                        "author": {
                            "@type": "Person",
                            "name": review.user_name
                        },
                        "datePublished": review.date,
                        "reviewRating": {
                            "@type": "Rating",
                            "ratingValue": review.total_score,
                            "bestRating": "5"
                        },
                        "positiveNotes": review.pros,
                        "negativeNotes": review.cons
                    });
                });

                // Calculate average rating
                if (reviewCount > 0) {
                    const averageRating = totalRating / reviewCount;

                    jsonLd.aggregateRating = {
                        "@type": "AggregateRating",
                        "ratingValue": Number(averageRating.toFixed(1)),
                        "reviewCount": reviewCount,
                        "bestRating": "5"
                    };
                }

                // Create and insert the script element
                const script = document.createElement('script');
                script.type = 'application/ld+json';
                script.text = JSON.stringify(jsonLd);
                document.head.appendChild(script);
            }

            function formatMotorNumber(number) {
                // Convert to string and split into integer and decimal parts
                const parts = number.toString().split('.');

                // Add commas to integer part
                parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');

                // Join back with decimal if it exists
                return parts.join('.');
            }

            // Second function: Compare cars from URL with Promise
            function compare_motors_from_url(modelId, variantId, container) {
                return new Promise((resolve, reject) => {
                    let selectedCarDetails = container.querySelector('.selected-car-details');
                    const addCar = container.querySelector('.add-car');
                    const selectCarText = container.querySelector('.select-car-text');
                    const switchButton = container.querySelector('.switch-button');

                    const variantNameElem = selectedCarDetails.querySelector('.variant-name');
                    const variantPriceElem = selectedCarDetails.querySelector('.variant-price');
                    const variantImage = selectedCarDetails.querySelector('.variant-image');

                    jQuery.ajax({
                        url: '<?php echo admin_url("admin-ajax.php"); ?>',
                        type: 'GET',
                        data: {
                            action: 'get_motor_compare_details',
                            variant_id: variantId,
                            model_id: modelId
                        },
                        success: function(response) {
                            if (response.success) {
                                selectedVariants.push(response.data.data);

                                const data = response.data.data;
                                variantImage.src = data.image_url;
                                variantImage.style.display = 'block';
                                variantNameElem.textContent = data.name;

                                let json_ld = response.data.data.json_ld;
                                if (json_ld) {
                                    generateMotorReviewsJsonLd(json_ld['title'], json_ld['user_reviews']);
                                }

                                let variant_price;
                                if (data.variant_data['price']) {
                                    variant_price = data.variant_data['price'];
                                    variant_price = variant_price == '' || variant_price == 0 ?
                                        'Belum Tersedia' : variant_price;
                                } else {
                                    variant_price = 'Belum Tersedia';
                                }
                                variantPriceElem.textContent = variant_price;

                                addCar.style.display = 'none';
                                selectCarText.style.display = 'none';
                                switchButton.style.display = 'block';

                                const cancelIcon = document.createElement('span');
                                cancelIcon.textContent = 'X';
                                cancelIcon.classList.add('cancel-icon');
                                selectedCarDetails.appendChild(cancelIcon);

                                cancelIcon.addEventListener('click', function() {
                                    const variantIndex = selectedVariants.findIndex(variant => variant.name === data.name);
                                    cancel_motor_Variant(variantIndex);
                                });

                                resolve(response.data); // Resolve the promise with the data
                            } else {
                                console.error("Failed to fetch variant details:", response.data.message);
                                reject(new Error(response.data.message));
                            }
                        },
                        error: function(xhr, status, error) {
                            console.error("AJAX Error:", error);
                            reject(error);
                        }
                    });
                });
            }
            document.querySelectorAll('.selects-car').forEach(container => updateMotorContainer(container));

            function updateMotorContainer(container) {
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
                    loadMotorbrands();
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
                                    selectedBrand = ' รุ่นรถ';
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

                            const modelId = item.dataset.modelId;
                            currentModelId = modelId;
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
                                        selectedModel = 'Varian';
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
                            const variantName = item.querySelector('.variant-name').textContent;
                            let selectedIndex;
                            const variantId = item.dataset.variantId;
                            const modelId = item.dataset.modelId;
                            // Send an AJAX request to fetch additional variant details (image and price)
                            const xhr = new XMLHttpRequest();
                            xhr.open('GET', '<?php echo admin_url('admin-ajax.php'); ?>?action=get_motor_compare_details&variant_id=' + variantId + '&model_id=' + modelId, true);
                            xhr.onload = function() {
                                if (xhr.status === 200) {
                                    const response = JSON.parse(xhr.responseText);
                                    if (response.success) {
                                        data = response.data.data
                                        variantImage.src = data.image_url;
                                        variantImage.style.display = 'block'; // Show the image
                                        variantNameElem.textContent = data.name;
                                        if (data.variant_data['price']) {
                                            $variant_price = data.variant_data['price'];

                                            if ($variant_price == '' || $variant_price == 0) {
                                                $variant_price = 'Belum Tersedia';
                                            } else {
                                                $variant_price = $variant_price;
                                            }
                                        } else {
                                            $variant_price = 'Belum Tersedia';
                                        }

                                        variantPriceElem.textContent = $variant_price;


                                        // Hide plus-sign and "Select Car"
                                        addCar.style.display = 'none';
                                        selectCarText.style.display = 'none';
                                        // console.log('new data', data);
                                        // Show switch button
                                        switchButton.style.display = 'block';

                                        // console.log('new selectedVariants', selectedVariants);

                                        const cancelIcon = document.createElement('span');
                                        cancelIcon.textContent = 'X'; // You can use an icon or character for this
                                        cancelIcon.classList.add('cancel-icon');
                                        selectedCarDetails.appendChild(cancelIcon);
                                        if (!isSwitching) {
                                            // If it's not a switch event, push the new variant into the selectedVariants array
                                            selectedVariants.push(response.data.data);
                                            // console.log('Added new variant to selectedVariants:', response.data.data);
                                        } else {
                                            // If it's a switch event, replace the variant in selectedVariants at the current index
                                            selectedVariants[currentCarIndex] = response.data.data;
                                            // console.log('Replaced variant at index', currentCarIndex, 'with', response.data.data);
                                        }
                                        // selectedVariants.push(response.data.data);
                                        updateBreadcrumb_motor();


                                        // In your main logic where you create the cancel icon
                                        cancelIcon.addEventListener('click', function() {
                                            // console.log('variant cancelled', variantName);
                                            const variantIndex = selectedVariants.findIndex(variant => variant.name === variantName);
                                            cancel_motor_Variant(variantIndex); // Call the reusable function
                                        });
                                        updateMotorComparison(selectedVariants);
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
                    loadMotorbrands();
                    // Hide the car details and plus sign
                    addCar.style.display = 'none';
                    selectCarText.style.display = 'none';


                    // console.log('selectedVariants before replacement', selectedVariants);


                    // Get the variant name to replace
                    const variantNameToReplace = variantNameElem.textContent.trim();


                    // Find the index of the car that needs to be replaced
                    const currentCarIndex = selectedVariants.findIndex(car => car.name === variantNameToReplace);


                    // console.log('currentCarIndex', currentCarIndex);


                    if (currentCarIndex !== -1) {
                        // Ensure we replace the current variant with a new one by fetching the new variant data
                        dropdown.addEventListener('click', function(event) {
                            const newItem = event.target.closest('.dropdown-item');
                            if (newItem && newItem.classList.contains('variant-item')) {
                                const newVariantId = newItem.dataset.variantId;
                                const newModelId = newItem.dataset.modelId;
                                // console.log(newVariantId, newModelId);
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

                                            // Update the UI with new variant details
                                            variantImage.src = newVariantData.image_url;
                                            variantNameElem.textContent = newVariantData.name;
                                            if (newVariantData.variant_data['price']) {
                                                $variant_price = newVariantData.variant_data['price'];

                                                if ($variant_price == '' || $variant_price == 0) {
                                                    $variant_price = 'Belum Tersedia';
                                                } else {
                                                    $variant_price = $variant_price;
                                                }
                                            } else {
                                                $variant_price = 'Belum Tersedia';
                                            }
                                            variantPriceElem.textContent = $variant_price;
                                            variantImage.style.display = 'block';


                                            // Update the breadcrumbs and hide dropdown
                                            updateMotorComparison(selectedVariants);


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
                            nextVariantPriceElem.textContent = nextVariant.variant_data['price'];
                            if (nextVariant.variant_data['price'] == '' || nextVariant.variant_data['price'] == 0) {
                                nextVariantPriceElem.textContent = 'Belum Tersedia';
                            } else {
                                nextVariantPriceElem.textContent = nextVariant.variant_data['price'];
                            }

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
                            loadMotorbrands();
                            updateMotorComparison(selectedVariants);
                        }
                    }
                }


                breadcrumbElement.addEventListener('click', function(event) {
                    const clickedText = event.target.textContent.trim();
                    // console.log('clickedText', clickedText);
                    if (clickedText === 'Merek' && selectedBrand !== '') {
                        dropdown.innerHTML = '';
                        dropdown.appendChild(breadcrumbElement);
                        loadMotorbrands();
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
                                updateBreadcrumb_motor();
                            }
                        };
                        xhr.send();
                    }
                });


                function loadMotorbrands() {
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
                    let breadcrumb = 'ยี่ห้อ';
                    if (selectedBrand) breadcrumb += ' > ' + selectedBrand;
                    if (selectedModel) breadcrumb += ' > ' + selectedModel;
                    if (selectedVariant) breadcrumb += ' > ' + selectedVariant;
                    breadcrumbElement.innerHTML = breadcrumb
                        .split(' > ')
                        .map((text, index) => `<span class="breadcrumb-item">${text}</span>`)
                        .join(' > ');
                }
            }

            function updateMotorComparison(selectedVariants) {
                const selectedCars = selectedVariants.filter(Boolean);
                // const comparisonTable = document.querySelector('.comparison-table');
                const comparisonTableBody = document.querySelector('.comparison-table tbody');
                const tabContainer = document.querySelector('.tab-container');
                const specDetails = document.querySelector('.spec-details');
                const comparisionTitle = document.querySelector('.comparision-title');
                const specTableBody = document.querySelector('.spec-table tbody');
                // const compareFButton = document.querySelector('.compare-button');
                const specsHead = document.querySelector('.specs-head');

                if (selectedCars.length < 2) {
                    document.querySelector('.error-message').style.display = 'block';
                    setTimeout(() => {
                        document.querySelector('.error-message').style.display = 'none';
                    }, 3000);
                    tabContainer.style.display = 'none';
                    specsHead.style.display = 'none';
                    // comparisionTitle.style.display = 'none';
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
                        extraDiv.innerHTML = `<h4>Daftar Varian</h4>`;

                        // Insert the extra div at the start of the selects-cars-wrapper
                        const wrapper = document.querySelector('.selects-cars-wrapper');
                        wrapper.insertBefore(extraDiv, wrapper.firstChild);
                    }

                    tabContainer.style.display = 'block'; // Hide the table
                    if (selectedModels.length > 1) {
                        comparisionTitle.textContent = selectedModels.join(' vs ');
                    }
                    //  selectedModels.join(' vs ');
                    comparisonTableBody.innerHTML = '';

                    const overview_specs = [{
                            label: 'Model',
                            key: 'body_type', // Top-level property in selectedCars[i]
                            source: 'car' // Specifies this is top-level in the selectedCars array
                        },
                        {
                            label: 'Kapasitas(cc)',
                            key: 'capacity', // From variant data
                            source: 'variant_data'
                        },
                        {
                            label: 'Tenaga Maksimal(hp)',
                            key: 'maximum_power', // From variant_data
                            source: 'variant_data'
                        },
                        {
                            label: 'Opsi start',
                            key: 'start_option', // From variant_data
                            source: 'variant_data'
                        },
                        {
                            label: 'Panel Instrumen',
                            key: 'instrument_panel', // From variant_data
                            source: 'variant_data'
                        },
                        {
                            label: 'ABS',
                            key: 'abs', // From variant_data
                            source: 'variant_data'
                        },
                        //                         {
                        //                             label: 'Overall User Rating',
                        //                             key: 'aggregated_user_rating',
                        //                             source: 'car'
                        //                         }
                    ];

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
                                    if (spec.key === 'aggregated_user_rating') {
                                        let rating = selectedCars[i]['aggregated_user_rating'];
                                        value = rating + '/5 ' + displayStarsMotors(rating);
                                    } else {
                                        value = selectedCars[i][spec.key] || '--';
                                    }
                                } else if (spec.source === 'listing_data') {
                                    // Data from listing_data
                                    value = selectedCars[i].listing_data?.[spec.key] || '--';
                                    if (value !== '--') {
                                        value = [...new Set(selectedCars[i].listing_data?.[spec.key])];
                                    }
                                } else if (spec.source === 'variant_data') {
                                    // Data from variant_data
                                    value = selectedCars[i].variant_data?.[spec.key] || '--';
                                }


                                dataCell.textContent = value;
                            } else {
                                dataCell.textContent = '--'; // Fallback if no car is selected
                            }

                            row.appendChild(dataCell);
                        }

                        // Append the row to the table body
                        comparisonTableBody.appendChild(row);
                    });

                    function createMotorDimensionRow(selectedCars) {
                        const row = document.createElement('tr');

                        // Create first column with dimension label
                        const labelCell = document.createElement('td');
                        labelCell.textContent = 'Komparasi Dimensi';
                        row.appendChild(labelCell);

                        // Create cells for each selected car
                        for (let i = 0; i < 4; i++) {
                            const dataCell = document.createElement('td');
                            if (selectedCars[i]) {
                                const container = document.createElement('div');
                                container.className = 'car-dimension-container';

                                // Side view section
                                const sideWrapper = document.createElement('div');
                                sideWrapper.className = 'dimension-wrapper';

                                const sideLength = document.createElement('div');
                                sideLength.className = 'dimension-value';
                                sideLength.textContent = `${selectedCars[i].length || '--'}`;

                                const sideImg = document.createElement('img');
                                sideImg.src = 'https://storage.googleapis.com/wp-my/malaysia/2025/01/16131347/car-side-view.svg';
                                sideImg.alt = 'Car Side View';
                                sideImg.className = 'dimension-img';

                                sideWrapper.appendChild(sideLength);
                                sideWrapper.appendChild(sideImg);

                                // Front view section
                                const frontWrapper = document.createElement('div');
                                frontWrapper.className = 'dimension-wrapper';

                                const frontWidth = document.createElement('div');
                                frontWidth.className = 'dimension-value width';
                                frontWidth.textContent = `${selectedCars[i].width || '--'}`;

                                const frontHeight = document.createElement('div');
                                frontHeight.className = 'dimension-value height';
                                frontHeight.textContent = `${selectedCars[i].height || '--'}`;

                                const frontImg = document.createElement('img');
                                frontImg.src = 'https://storage.googleapis.com/wp-my/malaysia/2025/01/16131340/car-front-view.svg';
                                frontImg.alt = 'Car Front View';
                                frontImg.className = 'dimension-img';

                                frontWrapper.appendChild(frontWidth);
                                frontWrapper.appendChild(frontHeight);
                                frontWrapper.appendChild(frontImg);

                                // Append both views to container
                                container.appendChild(sideWrapper);
                                container.appendChild(frontWrapper);
                                dataCell.appendChild(container);
                            }
                            row.appendChild(dataCell);
                        }

                        return row;
                    }

                    // CSS Styles
                    const styles = `
						.car-dimension-container {
							padding: 10px;
							text-align: center;
						}

						.dimension-wrapper {
							position: relative;
							margin: 20px 0;
							display: inline-block;
							width: 100%;
						}

						.dimension-img {
							max-width: 100%;
							height: auto;
							display: block;
							margin: 0 auto;
						}

						.dimension-value {
							position: absolute;
							font-size: 14px;
							color: #333;
							top: -25px;
							left: 50%;
							transform: translateX(-50%);
						}

						.dimension-value.width {
							top: auto;
							bottom: -25px;
						}

						.dimension-value.height {
							left: auto;
							right: -70px;
							top: 50%;
							transform: translateY(-50%);
						}
					`;

                    // Add styles
                    const styleSheet = document.createElement('style');
                    styleSheet.textContent = styles;
                    document.head.appendChild(styleSheet);

                    let selectedCarDimensions = [];
                    for (let i = 0; i < 4; i++) {
                        const dataCell = document.createElement('td');
                        if (selectedCars[i]) {
                            selectedCarDimensions.push({
                                'length': selectedCars?.[i]?.variant_data?.length?.[0] ?? null,
                                'width': selectedCars?.[i]?.variant_data?.width?.[0] ?? null,
                                'height': selectedCars?.[i]?.variant_data?.height?.[0] ?? null

                            })
                        } else {
                            selectedCarDimensions.push(null);
                        }
                    }

                    const dimensionRow = createMotorDimensionRow(selectedCarDimensions);
                    // Append to your table body
                    document.querySelector('tbody').appendChild(dimensionRow);

                    function createMotorPhotoComparisonRows(selectedCars) {
                        const rows = [];

                        // Create separate rows for exterior and interior
                        ['eksterior', 'warna'].forEach(type => {
                            const row = document.createElement('tr');

                            // Create label cell
                            const labelCell = document.createElement('td');
                            labelCell.textContent = type.charAt(0).toUpperCase() + type.slice(1);
                            row.appendChild(labelCell);

                            // Create cells for each selected car
                            for (let i = 0; i < 4; i++) {
                                const dataCell = document.createElement('td');
                                if (selectedCars[i] && selectedCars[i][type]) {
                                    const container = document.createElement('div');
                                    container.className = 'photo-gallery-container';

                                    // Create image container for each photo
                                    selectedCars[i][type].forEach((imgUrl, index) => {
                                        const imgWrapper = document.createElement('div');
                                        imgWrapper.className = 'photo-wrapper';

                                        const img = document.createElement('img');
                                        img.src = imgUrl;
                                        img.alt = `Car ${type} Photo ${index + 1}`;
                                        img.className = 'car-photo';

                                        // Add visual search icon
                                        const searchIcon = document.createElement('div');
                                        searchIcon.className = 'visual-search-icon';
                                        searchIcon.innerHTML = `
                        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor">
                            <path d="M15 15l6 6m-11-4a7 7 0 110-14 7 7 0 010 14z" stroke-width="2" stroke-linecap="round"/>
                        </svg>
                    `;

                                        // Optional: Add click handler for visual search
                                        imgWrapper.addEventListener('click', () => {
                                            // Handle visual search click
                                            // console.log(`Visual search clicked for ${type} image ${index + 1}`);
                                        });

                                        imgWrapper.appendChild(img);
                                        //                                         imgWrapper.appendChild(searchIcon);
                                        container.appendChild(imgWrapper);
                                    });

                                    dataCell.appendChild(container);
                                }
                                row.appendChild(dataCell);
                            }

                            rows.push(row);
                        });

                        return rows;
                    }

                    // CSS Styles
                    const photo_comparison_styles = `
                        .photo-gallery-container {
                            display: flex;
                            flex-direction: column;
                            gap: 15px;
                            padding: 10px;
                        }
                        
                        .photo-wrapper {
                            position: relative;
                            width: 100%;
                            cursor: pointer;
                            overflow: hidden;
                            border-radius: 4px;
                        }
                        
                        .photo-wrapper:hover .visual-search-icon {
                            opacity: 1;
                        }
                        
                        .car-photo {
                            width: 100%;
                            height: auto;
                            display: block;
                            transition: transform 0.3s ease;
                        }
                        
                        .photo-wrapper:hover .car-photo {
                            transform: scale(1.02);
                        }
                        
                        .visual-search-icon {
                            position: absolute;
                            top: 10px;
                            right: 10px;
                            background: rgba(255, 255, 255, 0.9);
                            border-radius: 50%;
                            padding: 8px;
                            opacity: 0;
                            transition: opacity 0.2s ease;
                            z-index: 2;
                        }
                        
                        .visual-search-icon svg {
                            width: 20px;
                            height: 20px;
                            stroke: #666;
                        }
                    `;

                    // Add styles
                    const photoComparisonStyleSheet = document.createElement('styles');
                    photoComparisonStyleSheet.textContent = photo_comparison_styles;
                    document.head.appendChild(photoComparisonStyleSheet);

                    let selectedCarPhotos = [];
                    for (let i = 0; i < 4; i++) {
                        const dataCell = document.createElement('td');
                        if (selectedCars[i]) {
                            selectedCarPhotos.push({
                                'eksterior': selectedCars?.[i]?.exterior_images ?? null,
                                'warna': selectedCars?.[i]?.interior_images ?? null,
                            })
                        } else {
                            selectedCarPhotos.push(null);
                        }
                    }

                    // Create and append both rows
                    const [exteriorRow, interiorRow] = createMotorPhotoComparisonRows(selectedCarPhotos);
                    const tableBody = document.querySelector('tbody');
                    tableBody.appendChild(exteriorRow);
                    tableBody.appendChild(interiorRow);

                    // Translation object mapping English specKeys to Thai
                    const translations = {
                        price: "Giá",
                        monthly_payment: "Trả Góp",
                        brand: "Thương hiệu",
                        model: "Dòng xe",
                        maximum_power: "Công suất tối đa(PS)",
                        year: "Năm sản xuất",
                        engine_type: "Loại động cơ",
                        start_option: "Bắt đầu các tùy chọn",
                        on_sale: "Khuyến mãi",
                        fuel_consumption: "Mức tiêu thụ nhiên liệu(L/100km)",
                        transmission: "Kiểu truyền tải",
                        fuel_type: "Loại nhiên liệu",
                        maximum_speed: "Tốc độ tối đa",
                        rpm_maximum_torque: "Mô-men xoắn cực đại RPM (RPM)",
                        number_of_cylinders: "số xi lanh",
                        rpm_maximum_power: "Công suất tối đa RPM (RPM)",
                        maximum_torque: "Mô-men xoắn cực đại(Nm)",
                        number_of_strokes: "Số kì",
                        capacity: "Dung tích(cc)",
                        length: "Dài(mm)",
                        height: "Cao(mm)",
                        width: "Rộng(mm)",
                        weight: "Trọng lượng(kg)",
                        seat: "Yên xe",
                        fuel_tank_capacity: "Dung tích bình xăng",
                        gear_box: "Hộp số",
                        transmission: "Kiểu truyền tải",
                        jenis_penggerak: "Loại ổ",
                        ground_clearance: "Khoảng sáng gầm xe",
                        chair_height: "Chiều cao yên",
                        rear_suspension: "Hệ thống treo sau",
                        front_suspension: "Hệ thống treo trước",
                        electronic_suspension_adjustment: "Điều chỉnh hệ thống treo điện tử",
                        head_lamp: "Đầu đèn",
                        indicator_light: "Đèn xi nhan",
                        taillight: "Đèn sau xe",
                        bbm_indicator: "Chỉ báo nhiên liệu",
                        speedometer: "Công tơ mét",
                        oil_change_indicator: "Đèn báo thay dầu",
                        instrument_panel: "Bảng điều khiển",
                        display_screen: "Màn hình hiển thị",
                        dimmer_switch: "Công tắc điều chỉnh độ sáng",
                        central_locking: "Khóa trung tâm",
                        rear_wheel_size: "Kích thước bánh sau",
                        front_wheel_size: "Kích thước bánh trước",
                        front_tire: "lốp trước",
                        rear_tire: "Lốp sau",
                        tire_type: "Loại lốp",
                        front_brake: "Phanh trước/Thắng trước",
                        rear_brake: "Phanh sau/thắng sau",
                        abs: "Hệ thống chống bó cứng phanh",
                        immobilizer: "Hệ thống chống trôm",
                        stability_control: "Kiểm soát ổn định",
                        engine_check_warning: "Cảnh báo kiểm tra động cơ",
                        alarm: "Báo thức",
                        front__rear_wheel_lock: "Khóa bánh trước / sau",
                        side_standard_indicator: "Đèn xi nhan",
                        traction_control: "Kiểm soát lực kéo",
                        driving_mode: "Chế độ lái",
                        cruise_control: "Kiểm soát hành trình",
                        adjustable_headlights: "Đèn pha có thể điều chỉnh",
                        changer_type: "Loại bộ sạc",
                        pack_capacity: "Dung lượng pi",
                        battery_charging_time: "thời lượng sạc pin",
                        pack_life: "Tuổi thọ pin",
                        pack_voltage: "Điện áp pin",
                        motor_type: "Loại động cơ",
                    };

                    const specKeys = new Set();
                    selectedCars.forEach(car => {
                        Object.keys(car.variant_data).forEach(key => {
                            if (!key.startsWith('_')) { // Ignore keys that start with '_'
                                specKeys.add(key);
                            }
                        });
                    });


                    const specs = Object.keys(translations);
                    specTableBody.innerHTML = '';


                    const numSelectedCars = selectedCars.length;


                    // Add rows for each specification
                    specs.forEach(specKey => {
                        const row = document.createElement('tr');


                        // Create the label cell
                        const specCell = document.createElement('td');
                        //                         specCell.textContent = specKey.replace(/_/g, ' ').replace(/\b\w/g, char => char.toUpperCase());
                        specCell.textContent = translations[specKey] || specKey.replace(/_/g, ' ').replace(/\b\w/g, char => char.toUpperCase());
                        row.appendChild(specCell);


                        // Add data cells, ensuring there are always 5 columns
                        for (let i = 0; i < 4; i++) {
                            const dataCell = document.createElement('td');
                            if (i < numSelectedCars) {
                                let value = selectedCars[i].variant_data[specKey] ? selectedCars[i].variant_data[specKey][0] : '--';
                                if (specKey === 'model') {
                                    value = selectedCars[i].listing_data['listing-model-name'][0];
                                }
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

            function displayStarsMotors(rating) {
                // Ensure rating is between 0 and 5
                rating = Math.max(0, Math.min(5, rating));

                const fullStars = Math.floor(rating);
                const hasHalfStar = false; //(rating - fullStars) >= 0.5;

                // Create stars using array methods for cleaner code
                const stars = [
                    ...Array(fullStars).fill('★'), // Full stars
                    ...(hasHalfStar ? ['⯨'] : []), // Half star if needed
                    ...Array(5 - fullStars - (hasHalfStar ? 1 : 0)).fill('☆') // Empty stars
                ].join('');

                return stars;
            }


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

// Shortcode for displaying the motor selection
add_shortcode('compare_motor_variant', 'selects_motor_shortcode');


add_action('wp_ajax_compare_motors_action', 'handle_compare_motors_ajax');
add_action('wp_ajax_nopriv_compare_motors_action', 'handle_compare_motors_ajax');

function handle_compare_motors_ajax()
{
    // Get the car slugs from the AJAX request
    $car1 = sanitize_text_field($_POST['car1']);
    $car2 = sanitize_text_field($_POST['car2']);

    // Query posts of type 'listing' with the provided slugs
    $args = [
        'post_type' => 'motorcycle-listing',
        'post_status' => 'publish',
        'posts_per_page' => -1,
        'post_name__in' => [$car1, $car2], // Match the post_name (slug)
    ];

    $query = new WP_Query($args);

    if ($query->have_posts()) {
        $listings = [];

        while ($query->have_posts()) {
            $query->the_post();
            $listing_id = get_the_ID();

            // Get the first variant for this listing
            $variant_args = [
                'post_type'   => 'motorcycle-variant',
                'post_status' => 'publish',
                'post_parent' => $listing_id, // Ensure the parent is the current listing
                'posts_per_page' => 1,        // Only fetch the first variant
                'orderby' => 'ID',           // Order by ID (earliest created variant)
                'order' => 'ASC'
            ];

            $variant_query = new WP_Query($variant_args);
            $variant_data = null;

            if ($variant_query->have_posts()) {
                $variant_query->the_post();
                $variant_id = get_the_ID();

                // Get all post meta for this variant
                $variant_meta = get_post_meta($variant_id);

                // Add variant data
                $variant_data = [
                    'variant_id'    => $variant_id,
                    'variant_title' => get_the_title(),
                    'variant_url'   => get_permalink(),
                    'variant_meta'  => $variant_meta,
                ];

                wp_reset_postdata(); // Reset after the variant query
            }

            // Add listing data along with its first variant
            $listings[] = [
                // 'post_name' => get_post_field('post_name'),
                // 'post_title' => get_the_title(),
                // 'post_url' => get_permalink(),
                // 'variant' => $variant_data, // Add variant data here
                'model_id' => $listing_id,
                'variant_id' => $variant_id
            ];
        }

        wp_send_json_success(['listings' => $listings]);
    } else {
        wp_send_json_error(['message' => 'No listings found']);
    }

    wp_die(); // Always include this to properly terminate the request
}



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
        WHERE tt.taxonomy = 'motorcycle_make' AND tt.parent = 0
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
        WHERE pm.meta_key = 'make'
        AND pm.meta_value = %d
        ORDER BY p.post_title
    ", $brand_id));

    $response = [];
    $model_ids = [];
    foreach ($car_models as $model) {
        $model_id = $model->ID;
        $model_name = $model->post_title;
        if (!in_array($model_id, $model_ids)) {
            $response[] = [
                'ID' => $model->ID,
                'post_title' => $model_name,
            ];
            $model_ids[] = $model_id;
        }
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
    WHERE pm.meta_key = 'model' AND p.post_type = 'motorcycle-variant' AND pm.meta_value LIKE %s
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
    $price = isset($variant_meta['price'][0]) ? $variant_meta['price'][0] : null;

    // Format the retail price in PHP
    $formatted_price = $price ? format_price_vietnam($price) : 'Belum Tersedia';

    // $image_id = get_post_meta($variant_id, 'image', true);
    $body_type_id = get_post_meta($model_id, 'listing_type', true);
    $body_type_term = get_term($body_type_id);
    $body_type_name = is_wp_error($body_type_term) ? '--' : $body_type_term->name;

    $listing_data = get_post_meta($model_id);
    // Get the image URL and GUID
    //   $image_url = $image_id ? wp_get_attachment_url($image_id) : '';
    //   $image_post = get_post($image_id);
    //   $image_guid = $image_post ? $image_post->guid : '';
    $post_thumbnail_id = get_post_thumbnail_id($variant_id);
    $thumbnail_post = get_post($post_thumbnail_id);
    $guid = $thumbnail_post->guid;
    $aggregated_user_rating_response = get_motor_aggregated_user_rating($variant_id);
    $aggregated_user_rating = $aggregated_user_rating_response['average_total_rating'] ?? 0;
    $json_ld = $aggregated_user_rating_response['json_ld'];
    $images = get_motor_interior_exterior_images_of_variant($variant_id);
    // $image_url = $image_id ? wp_get_attachment_url($image_id) : '';
    if ($variant_post) {
        $response = [
            'success' => true,
            'data' => [
                'variant_data' => array_merge($variant_meta, ['price' => $formatted_price]),
                'name' => $variant_post->post_title,
                'image_url' => $guid,
                'body_type' => $body_type_name ?? '--',
                // 'segment' => $segment ?? '--',
                'listing_data' => $listing_data,
                'id' => $variant_id,
                'aggregated_user_rating' => $aggregated_user_rating,
                'json_ld' => $json_ld,
                'exterior_images' => $images['Eksterior'],
                'interior_images' => $images['Warna']
            ]
        ];
        wp_send_json_success($response);
    } else {
        wp_send_json_error('Variant not found');
    }

    wp_die();
}

function get_motor_interior_exterior_images_of_variant($variant_id)
{
    global $wpdb;
    $sql = $wpdb->prepare(
        "SELECT colour, type, image_data FROM car_image WHERE variant_post_id = %d",
        $variant_id
    );

    $images = $wpdb->get_results($sql);

    $tabs = [
        'Eksterior' => [],
        'Warna' => [],
    ];

    foreach ($images as $image) {
        $imageDataArray = json_decode($image->image_data);
        if ($imageDataArray) {
            foreach ($imageDataArray as $imgData) {
                $type = $image->type === 'Exterior' ? 'Eksterior' : ($image->type === 'Colour' ? 'Warna' : null);
                // Make sure the URL exists and limit the count to 5
                if ($type && isset($imgData->url) && isset($tabs[$type])) {
                    if (count($tabs[$type]) < 5) {
                        $tabs[$type][] = $imgData->url;
                    } else {
                        break; // Stop adding more images once the limit is reached
                    }
                }
            }
        }
    }

    return $tabs;
}

function get_motor_aggregated_user_rating($variant_id)
{
    require_once(get_stylesheet_directory() . '/json-ld/review-json-ld.php');

    $variant_post = get_post($variant_id);

    $total_user_reviews = 0;
    $average_total_rating = 0;
    if ($variant_post) {
        // get all User-Review posts whose post_parent is the current variant post
        $user_review_posts = get_posts(array(
            'post_parent' => $variant_id,
            'post_type' => 'user-review',
            'posts_per_page' => -1
        ));

        $user_reviews = [];
        $json_ld = [];
        foreach ($user_review_posts as $user_review_post) {
            $total_user_reviews += 1;
            $average_total_rating += get_post_meta($user_review_post->ID, 'total_score', true);
            $user_id = get_post_meta($user_review_post->ID, 'user', true);
            $user = get_user_by('ID', $user_id);
            $user_reviews[] = array(
                'user_name' => $user->display_name,
                'date' => get_post_meta($user_review_post->ID, 'date', true),
                'total_score' => get_post_meta($user_review_post->ID, 'total_score', true),
                'price_score' => get_post_meta($user_review_post->ID, 'price', true),
                'performance_score' => get_post_meta($user_review_post->ID, 'performance', true),
                'ride_comfort_score' => get_post_meta($user_review_post->ID, 'ride_comfort', true),
                'space_score' => get_post_meta($user_review_post->ID, 'space', true),
                'fuel_economy_score' => get_post_meta($user_review_post->ID, 'fuel_economy', true),
                'variant_name' => get_the_title(),
                // 'fuel_economy_name' => get_the_title(),
                'pros' => get_post_meta($user_review_post->ID, 'pros', true),
                'cons' => get_post_meta($user_review_post->ID, 'cons', true),
            );
        }

        if ($total_user_reviews == 0) {
            return 0;
        }

        // order by date desc and slice the first 2
        usort($user_reviews, function ($a, $b) {
            return strtotime($b['date']) - strtotime($a['date']);
        });
        $user_reviews = array_slice($user_reviews, 0, 2);
        $average_total_rating = round($average_total_rating / $total_user_reviews, 1);

        $json_ld = ['title' => $variant_post->post_title, 'user_reviews' => $user_reviews];
        // generate_reviews_json_ld($variant_post->post_title, $user_reviews);
    }

    return [
        'average_total_rating' => $average_total_rating,
        'json_ld' => $json_ld
    ];
}

function displayStarsMotors($rating)
{
    $rating = max(0, min(5, $rating)); // Ensure rating is between 0 and 5
    $fullStars = floor($rating);
    $hasHalfStar = ($rating - $fullStars) >= 0.5;

    $stars = str_repeat('★', $fullStars); // Full stars
    if ($hasHalfStar) {
        $stars .= '⯨'; // Half star
    }
    $stars .= str_repeat('☆', 5 - $fullStars - ($hasHalfStar ? 1 : 0)); // Empty stars

    return $stars;
}
?>