<?php

// import gallary-page.css
function enqueue_motor_gallery_page_css()
{
        wp_enqueue_style('gallery-page-styles', get_stylesheet_directory_uri() . '/Gallary/css/gallery-page.css');
}

function motor_gallery_shortcode()
{
    enqueue_motor_gallery_page_css();

    $global_listing_post_data = get_motor_listing_from_query_vars();
    $listing_post = $global_listing_post_data['post'];
    $post_title = $listing_post->post_title;
    $variants = $global_listing_post_data['variant_posts'];
    $images = $global_listing_post_data['image_data'];

    $make_slug = $global_listing_post_data['listing_make_term']->slug;
    $model_slug = $listing_post->post_name;
    $base_url = home_url('/motorcycles/') . $make_slug . '/' . $model_slug . '/';

    if (empty($images)) {
        return;
    }

    $tabs = [
        'Exterior' => [],
        'Colour' => []
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
    <div class="gallery-tabs-container container">
        <div class="dropdown-container">
            <h3><?php echo $post_title; ?></h3>
            <!-- Dropdown for Switch Variants -->
            <div class="custom-dropdown">
                <select name="car_variant" id="car_variant_dropdown" onchange="location.href = this.value;">
                    <option value=""> Đổi mẫu xe</option>
                    <?php foreach ($variants as $variant): ?>
                        <option value="<?php echo $base_url . $variant->post_name . '/gallery'; ?>">
                            <?php echo $variant->post_title; ?>
                        </option>
                    <?php endforeach; ?>
                </select>
            </div>
        </div>
        <!-- Tabs -->
        <ul class="nav nav-tabs" id="galleryTab" role="tablist">
            <?php foreach ($tabs as $tabName => $images): ?>
                <li class="nav-item">
                    <a class="nav-link <?php echo $tabName === 'Exterior' ? 'active' : ''; ?>" id="<?php echo strtolower($tabName); ?>-tab" data-toggle="tab" href="#<?php echo strtolower($tabName); ?>" role="tab" onclick="setActiveTab('<?php echo strtolower($tabName); ?>')">
                        <?php if ($tabName === 'Exterior') {
                            echo 'Ngoại thất';
                        } else {
                            echo 'Màu sắc';
                        }; ?>
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

    <script>
        let currentSlides = {
            'exterior': 0,
            'colour': 0,
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
add_shortcode('motor_gallery', 'motor_gallery_shortcode');
