<?php

function variant_gallery_shortcode()
{
    // global $post;
    global $wpdb;

    $global_variant_post_data = get_variant_from_query_vars();
    if (!$global_variant_post_data) {
        return;
    }

    $listing_post = $global_variant_post_data['listing_post'];
    $listing_post_id = $listing_post->ID;

    $current_variant_post = $global_variant_post_data['variant_post'];
    $current_variant_id = $current_variant_post->ID;

    if (!$current_variant_post) {
        return;
    }

    $all_variant_ids = $wpdb->get_results($wpdb->prepare(
        "SELECT ID, post_title, post_name FROM {$wpdb->posts} WHERE post_parent = %d AND post_type = 'variant'",
        $listing_post_id
    ));

    $filtered_variants = [];
    foreach ($all_variant_ids as $variant) {
        $state = get_post_meta($variant->ID, 'state', true); // Replace 'state' with your actual meta key if different
        if ($state == 1) {
            $filtered_variants[] = $variant; // Keep only the variants where state is 1
        }
    }

    $sql = $wpdb->prepare(
        "SELECT colour, type, image_data FROM car_image WHERE variant_post_id=%d",
        $current_variant_id
    );

    $images = $wpdb->get_results($sql);

    if (empty($images)) {
        return showSwitchVariant($listing_post, $filtered_variants);
    }

    $tabs = [
        'Exterior' => [],
        'Interior' => [],
        'Others' => []
    ];

    foreach ($images as $image) {
        $imageDataArray = json_decode($image->image_data);
        if ($imageDataArray) {
            foreach ($imageDataArray as $imgData) {
                // Make sure the URL exists before adding it
                if (isset($imgData->url) && isset($tabs[$image->type])) {
                    $tabs[$image->type][] = $imgData->url;
                }
            }
        }
    }

    ob_start();
?>
    <div class="gallery-tabs-container">
        <?php echo showSwitchVariant($listing_post, $filtered_variants); ?>
        <!-- Tabs -->
        <ul class="nav nav-tabs" id="galleryTab" role="tablist">
            <?php foreach ($tabs as $tabName => $images): ?>
                <li class="nav-item">
                    <a class="nav-link <?php echo $tabName === 'Exterior' ? 'active' : ''; ?>" id="<?php echo strtolower($tabName); ?>-tab" data-toggle="tab" href="#<?php echo strtolower($tabName); ?>" role="tab" onclick="setActiveTab('<?php echo strtolower($tabName); ?>')">
                        <?php echo $tabName; ?>
                    </a>
                </li>
            <?php endforeach; ?>
        </ul>

        <!-- Tab content -->
        <div class="tab-content">
            <?php foreach ($tabs as $tabName => $images): ?>
                <div class="tab-pane <?php echo $tabName === 'Exterior' ? 'show active' : ''; ?>" id="<?php echo strtolower($tabName); ?>" role="tabpanel">
                    <div class="row">
                        <!-- Carousel -->
                        <div class="col-md-10">
                            <div id="<?php echo strtolower($tabName); ?>Carousel" class="carousel" style="max-height: 100%; overflow: hidden; width:100%">
                                <div class="carousel-inner">
                                    <?php foreach ($images as $index => $image): ?>
                                        <div class="carousel-item <?php echo $index === 0 ? 'active' : ''; ?>">
                                            <img src="<?php echo $image; ?>" class="d-block big-image" alt="<?php echo $tabName; ?> Image <?php echo $index + 1; ?>">
                                        </div>
                                    <?php endforeach; ?>
                                </div>

                                <button class="carousel-control-prev" type="button" onclick="prevSlide('<?php echo strtolower($tabName); ?>')">
                                    &#10094;
                                </button>
                                <button class="carousel-control-next" type="button" onclick="nextSlide('<?php echo strtolower($tabName); ?>')">
                                    &#10095;
                                </button>
                            </div>
                        </div>

                        <!-- Thumbnails -->
                        <div class="col-md-2 d-flex flex-column align-items-start thumbnail-container">
                            <?php foreach ($images as $index => $image): ?>
                                <img src="<?php echo $image; ?>" class="img-thumbnail mb-2" alt="Thumb <?php echo $index + 1; ?>" onclick="setActiveSlide(<?php echo $index; ?>, '<?php echo strtolower($tabName); ?>')">
                            <?php endforeach; ?>
                        </div>
                    </div>
                </div>
            <?php endforeach; ?>
        </div>
    </div>

    <style>
        .nav-tabs li a {
            margin-right: 2px;
            font-size: 16px;
            font-weight: 700;
            line-height: 1.9;
            border: 1px solid transparent;
            border-radius: 8px 8px 0 0;
        }

        .nav-tabs {
            margin-bottom: 10px;
        }

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
            border-bottom: 3px solid #32D0C6;
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
            border-bottom: 3px solid #32D0C6 !important;
            /* Only bottom border */
            background-color: transparent !important;
            border-left: none !important;
            border-right: none !important;
            border-top: none !important;
        }

        .carousel {
            position: relative;
        }

        .carousel-inner {
            position: relative;
            width: 100%;
            overflow: hidden;
        }

        .carousel-item {
            display: none;
            text-align: center;
        }

        .carousel-item.active {
            display: block;
        }

        .carousel-control-prev,
        .carousel-control-next {
            position: absolute;
            top: 50%;
            transform: translateY(-50%);
            background-color: rgba(0, 0, 0, 0.7);
            color: white;
            padding: 10px;
            cursor: pointer;
            z-index: 1;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            display: flex;
            justify-content: center;
            align-items: center;
            font-size: 20px;
        }

        .carousel-control-prev {
            left: 10px;
        }

        .carousel-control-next {
            right: 10px;
        }

        .img-thumbnail {
            cursor: pointer;
            width: 80%;
            height: 90px;
            object-fit: cover;
            margin-bottom: 10px;
        }

        .thumbnail-container .img-thumbnail.highlighted {
            border: 4px solid #32d0c6;
        }

        .thumbnail-container {
            height: 650px;
            overflow-y: scroll;
            padding: 10px 0;
        }

        .thumbnail-container::-webkit-scrollbar {
            display: none;
        }

        .big-image {
            height: 650px !important;
            width: 1040px !important;
        }

        .nav-tabs li a {
            position: relative;
            display: block;
            padding: 6px 11px;
        }
    </style>

    <script>
        let currentSlides = {
            'exterior': 0,
            'interior': 0,
            'others': 0,
        };

        function updateThumbnailHighlight(index, tab) {
            var thumbnails = document.querySelectorAll('#' + tab + ' .thumbnail-container .img-thumbnail');
            thumbnails.forEach(function(thumb, i) {
                thumb.classList.remove('highlighted');
            });
            thumbnails[index].classList.add('highlighted');
        }

        function setActiveSlide(index, tab) {
            var items = document.querySelectorAll('#' + tab + ' .carousel-item');
            items.forEach(function(item, i) {
                item.classList.remove('active');
            });
            items[index].classList.add('active');
            updateThumbnailHighlight(index, tab);
            currentSlides[tab] = index;
        }

        function prevSlide(tab) {
            var length = document.querySelectorAll('#' + tab + ' .carousel-item').length;
            currentSlides[tab] = (currentSlides[tab] === 0) ? length - 1 : currentSlides[tab] - 1;
            setActiveSlide(currentSlides[tab], tab);
        }

        function nextSlide(tab) {
            var length = document.querySelectorAll('#' + tab + ' .carousel-item').length;
            currentSlides[tab] = (currentSlides[tab] === length - 1) ? 0 : currentSlides[tab] + 1;
            setActiveSlide(currentSlides[tab], tab);
        }

        function setActiveTab(tab) {
            // Remove 'active' class from all tabs
            var tabs = document.querySelectorAll('.nav-link');
            tabs.forEach(function(tabLink) {
                tabLink.classList.remove('active');
            });

            // Set the 'active' class on the clicked tab
            var activeTabLink = document.querySelector('[href="#' + tab + '"]');
            activeTabLink.classList.add('active');

            // Activate tab content
            var panes = document.querySelectorAll('.tab-pane');
            panes.forEach(function(pane) {
                pane.classList.remove('show', 'active');
            });

            var activePane = document.getElementById(tab);
            activePane.classList.add('show', 'active');

            // Set the first slide as active for the selected tab
            setActiveSlide(0, tab);
        }

        document.addEventListener('DOMContentLoaded', function() {
            // Activate the exterior tab and its content on page load
            setActiveTab('exterior'); // Display the content for the "Exterior" tab
            setActiveSlide(0, 'exterior'); // Make sure the first slide of the "Exterior" tab is active
            updateThumbnailHighlight(0, 'exterior'); // Highlight the first thumbnail for the "Exterior" tab
        });
    </script>

<?php
    return ob_get_clean();
}
add_shortcode('variant_gallery', 'variant_gallery_shortcode');

function showSwitchVariant($listing_post, $filtered_variants)
{
    ob_start();
?>
    <div class="dropdown-container">
        <h3><?php echo $listing_post->post_title; ?></h3>
        <!-- Dropdown for Switch Variants -->
        <div class="custom-dropdown">
            <select name="car_variant" id="car_variant_dropdown">
                <option value="">Switch Variant</option>
                <?php foreach ($filtered_variants as $variant): ?>
                    <option value="<?php echo $variant->post_name; ?>"><?php echo $variant->post_title; ?></option>
                <?php endforeach; ?>
            </select>
        </div>
    </div>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            var dropdown = document.getElementById('car_variant_dropdown');
            dropdown.addEventListener('change', function() {
                switchVariant(this.value);
            });
        })


        function switchVariant($post_name) {
            var currentUrl = window.location.href;
            var urlParts = currentUrl.split('/').filter(Boolean);
            const acceptedLastParts = ['specs', 'gallery', 'overview'];

            if (acceptedLastParts.includes(urlParts[urlParts.length - 1])) {
                urlParts[urlParts.length - 2] = $post_name;
            } else {
                urlParts.pop();
                urlParts.push($post_name);
            }

            var newUrl = '/' + urlParts.slice(2).join('/');
            window.location.href = newUrl;
        }
    </script>
    <style>
        /* switch variant Dropdown design */
        .dropdown-container {
            display: flex;
            align-items: center;
            gap: 31px;
        }

        .custom-dropdown {
            position: relative;
            display: inline-block;
            font-family: Arial, sans-serif;
        }

        /* Style the select element */
        .custom-dropdown select {
            appearance: none;
            -webkit-appearance: none;
            -moz-appearance: none;
            background-color: #fff;
            border: 1px solid #ccc;
            border-radius: 4px;
            padding: 5px 10px;
            font-size: 16px;
            font-weight: bold;
            color: #333;
            cursor: pointer;
        }

        /* Placeholder styling for the "Switch Variant" option */
        .custom-dropdown select option[value=""] {
            color: #888;
            font-weight: normal;
        }

        /* Add a custom down arrow */
        .custom-dropdown::after {
            content: '▼';
            position: absolute;
            right: 10px;
            top: 50%;
            transform: translateY(-50%);
            font-size: 12px;
            color: #333;
            pointer-events: none;
        }

        /* Hide the default arrow in some browsers */
        .custom-dropdown select::-ms-expand {
            display: none;
        }
    </style>
<?php
    $output = ob_get_clean();

    return $output;
}
