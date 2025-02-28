<?php
// import exterior-images.css
function motor_gallery_exterior_images_css()
{
    wp_enqueue_style('exterior-images', get_stylesheet_directory_uri() . '/Gallary/css/exterior-images.css');
}

// import engine-and-other-images.css
function motor_gallery_engineandother_images_css()
{
    wp_enqueue_style('engine-and-other-images', get_stylesheet_directory_uri() . '/Gallary/css/engine-and-other-images.css');
}

// import highlights-images.css
function motor_gallery_highlights_images_css()
{
    wp_enqueue_style('highlights-images', get_stylesheet_directory_uri() . '/Gallary/css/highlights-images.css');
}

// Exterior images shortcode
function motor_gallery_exterior_shortcode()
{
    motor_gallery_exterior_images_css();

    $global_listing_post_data = get_motor_listing_from_query_vars();
    $listing_post = $global_listing_post_data['post'];
    $post_title = $listing_post->post_title;
    $post_name = $listing_post->post_name;
    $data = $global_listing_post_data['image_data'];
    $current_url = $_SERVER['REQUEST_URI'];

    if (empty($data)) {
        return;
    }

    // Process images for the initial load
    $images = [];
    $exteriorIndex = 1;
    foreach ($data as $item) {
        if ($item->type === 'Exterior') {
            $image_data = json_decode($item->image_data);
            foreach ($image_data as $image) {
                $images[] = [
                    'src' => $image->url,
                    'alt' => $post_title . ' Exterior ' . str_pad($exteriorIndex++, 3, '0', STR_PAD_LEFT),
                    'title' => $post_title . ' ภายนอก ' . str_pad($exteriorIndex - 1, 3, '0', STR_PAD_LEFT),
                    // 'link' => '/cars/honda/hr-v/car-exterior-image-' . $exteriorIndex
                    'link' => $current_url
                ];
            }
        }
    }

    $totalImages = count($images);
    $initialImages = array_slice($images, 0, 9); // Initial 9 images

    // Start building the output
    ob_start();
?>
    <h2 class="wa-title-text"><?php esc_html_e('รูปภาพ & ภาพถ่าย ' . $post_title, 'voiture'); ?></h2>

    <div class="gallery-list" data-total-images="<?php echo esc_attr($totalImages); ?>" data-listing-name="<?php echo esc_attr($post_name); ?>">
        <?php
        foreach ($initialImages as $image) { ?>
            <div class="gallery-list-item">
                <a href="<?php echo esc_url($image['link']); ?>">
                    <img src="<?php echo esc_url($image['src']); ?>" title="<?php echo esc_attr($image['title']); ?>" alt="<?php echo esc_attr($image['alt']); ?>">
                </a>
                <div>
                    <a href="<?php echo esc_url($image['link']); ?>" class="description-link"><?php echo esc_html($image['title']); ?></a>
                </div>
            </div>
        <?php } ?>
    </div>

    <?php if ($totalImages > 9) : ?>
        <div class="view-more-container">
            <button class="view-more" data-offset="9" data-total="<?php echo esc_attr($totalImages); ?>">
                ดูเพิ่มเติม
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                </svg>
            </button>
        </div>
    <?php endif; ?>

    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const viewMoreButton = document.querySelector('.view-more');
            let offset = parseInt(viewMoreButton.getAttribute('data-offset'));
            const totalImages = parseInt(viewMoreButton.getAttribute('data-total'));
            const galleryList = document.querySelector('.gallery-list');

            viewMoreButton.addEventListener('click', function() {
                if (offset < totalImages) {
                    // Load the next 9 images
                    const newImages = <?php echo json_encode($images); ?>.slice(offset, offset + 9);
                    newImages.forEach(image => {
                        const item = document.createElement('div');
                        item.classList.add('gallery-list-item');
                        item.innerHTML = `
                            <a href="${image.link}">
                                <img src="${image.src}" title="${image.title}" alt="${image.alt}">
                            </a>
                            <div>
                                <a href="${image.link}" class="description-link">${image.title}</a>
                            </div>
                        `;
                        galleryList.appendChild(item);
                    });
                    offset += 9; // Update offset
                    viewMoreButton.setAttribute('data-offset', offset); // Update button data attribute
                }

                // Hide button if all images are loaded
                if (offset >= totalImages) {
                    viewMoreButton.style.display = 'none';
                }
            });
        });
    </script>
<?php

    return ob_get_clean();
}
add_shortcode('motor_images', 'motor_gallery_exterior_shortcode');


// Engine and other images shortcode
function motor_gallery_engineandother_shortcode()
{
    motor_gallery_engineandother_images_css();

    $global_listing_post_data = get_motor_listing_from_query_vars();
    $listing_post = $global_listing_post_data['post'];
    $post_title = $listing_post->post_title;
    $data = $global_listing_post_data['image_data'];
    $current_url = $_SERVER['REQUEST_URI'];

    if (empty($data)) {
        return;
    }

    $other_images = [];
    $othersIndex = 1;

    // Static array with image data
    foreach ($data as $item) {
        if ($item->type === 'Colour') {
            $image_data = json_decode($item->image_data);
            foreach ($image_data as $image) {
                $other_images[] = [
                    'src' => $image->url,
                    'alt' => $post_title . ' Colour ' . str_pad($othersIndex++, 3, '0', STR_PAD_LEFT),
                    'title' => $post_title . ' สี ' . str_pad($othersIndex - 1, 3, '0', STR_PAD_LEFT),
                    // 'link' => '/cars/honda/hr-v/car-others-image-' . $othersIndex
                    'link' => $current_url
                ];
            }
        }
    }
    $totalOtherImages = count($other_images);
    $displayedOtherImages = array_slice($other_images, 0, 9);

    // Generate HTML output
    ob_start();
?>
    <h2 class="wa-title-text"><?php esc_html_e('สีสัน ' . $post_title, 'voiture'); ?></h2>

    <div class="other-img-gallery-list">
        <?php foreach ($displayedOtherImages as $image) { ?>
            <div class="other-img-gallery-list-item">
                <a href="<?php echo esc_url($image['link']); ?>">
                    <img src="<?php echo esc_url($image['src']); ?>" title="<?php echo esc_attr($image['title']); ?>" alt="<?php echo esc_attr($image['alt']); ?>">
                </a>
                <div>
                    <a href="<?php echo esc_url($image['link']); ?>" class="other-img-description-link"><?php echo esc_html($image['title']); ?></a>
                </div>
            </div>
        <?php } ?>
    </div>

    <?php if ($totalOtherImages > 9) : ?>
        <div class="other-img-view-more-container">
            <button class="other-img-view-more" data-offset="9" data-total="<?php echo esc_attr($totalImages); ?>">
                ดูเพิ่มเติม
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                </svg>
            </button>
        </div>
    <?php endif; ?>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const viewMoreButton = document.querySelector('.other-img-view-more');
            let offset = parseInt(viewMoreButton.getAttribute('data-offset'));
            const totalImages = parseInt(viewMoreButton.getAttribute('data-total'));
            const galleryList = document.querySelector('.other-img-gallery-list');

            viewMoreButton.addEventListener('click', function() {
                if (offset < totalImages) {
                    // Load the next 9 images
                    const newImages = <?php echo json_encode($other_images); ?>.slice(offset, offset + 9);
                    newImages.forEach(image => {
                        const item = document.createElement('div');
                        item.classList.add('other-img-gallery-list-item');
                        item.innerHTML = `
                    <a href="${image.link}">
                        <img src="${image.src}" title="${image.title}" alt="${image.alt}">
                    </a>
                    <div>
                        <a href="${image.link}" class="other-img-description-link">${image.title}</a>
                    </div>
                `;
                        galleryList.appendChild(item);
                    });
                    offset += 9; // Update offset
                    viewMoreButton.setAttribute('data-offset', offset); // Update button data attribute
                }

                // Hide button if all images are loaded
                if (offset >= totalImages) {
                    viewMoreButton.style.display = 'none';
                }
            });
        });
    </script>
<?php
    return ob_get_clean();
}
add_shortcode('motor_engine_and_other_images', 'motor_gallery_engineandother_shortcode');


// Highlight images shortcode
function motor_gallery_highlights_shortcode()
{
    motor_gallery_highlights_images_css();

    $global_listing_post_data = get_motor_listing_from_query_vars();
    $listing_post = $global_listing_post_data['post'];
    $post_title = $listing_post->post_title;
    $data = $global_listing_post_data['image_data'];
    $current_url = $_SERVER['REQUEST_URI'];

    if (empty($data)) {
        return;
    }

    $images = [
        // 'interior' => [],
        'exterior' => [],
        'colour' => []
    ];

    // Collect images based on type
    foreach ($data as $item) {
        $image_data = json_decode($item->image_data);
        foreach ($image_data as $image) {
            // Limit to 3 images for each type
            if ($item->type === 'Exterior' && count($images['exterior']) < 3) {
                $index = str_pad(count($images['exterior']) + 1, 3, '0', STR_PAD_LEFT);
                $images['exterior'][] = [
                    'src' => $image->url,
                    'alt' => $post_title . ' Exterior ' . $index,
                    'title' => $post_title . ' ภายนอก ' . $index,
                    // 'link' => '/cars/honda/hr-v/car-exterior-image-' . $index
                    'link' => $current_url
                ];
            } elseif ($item->type === 'Colour' && count($images['colour']) < 3) {
                $index = str_pad(count($images['colour']) + 1, 3, '0', STR_PAD_LEFT);
                $images['colour'][] = [
                    'src' => $image->url,
                    'alt' => $post_title . ' Colour ' . $index,
                    'title' => $post_title . ' สี ' . $index,
                    // 'link' => '/cars/honda/hr-v/car-others-image-' . $index
                    'link' => $current_url
                ];
            }
        }
    }
    // Generate HTML output
    ob_start();
?>
    <h2 class="wa-title-text"><?php esc_html_e('การดีไซน์ไฮไลท์ ' . $post_title, 'voiture'); ?></h2>


    <div class="gallery-list">
        <?php foreach ($images as $image_type => $image_array) {
            foreach ($image_array as $img) { ?>
                <div class="gallery-list-item">
                    <a href="<?php echo esc_url($img['link']); ?>">
                        <img src="<?php echo esc_url($img['src']); ?>" title="<?php echo esc_attr($img['title']); ?>" alt="<?php echo esc_attr($img['alt']); ?>">
                    </a>
                    <div>
                        <a href="<?php echo esc_url($img['link']); ?>" class="description-link"><?php echo esc_html($img['title']); ?></a>
                    </div>
                </div>
        <?php }
        } ?>

    </div>
    <!-- <div class="view-more-container">
        <a href="/cars/honda/hr-v/gallery" class="view-more-link">View More</a>
    </div> -->
    <style>
        /* Reuse the same styling */
    </style>
<?php
    return ob_get_clean();
}
add_shortcode('motor_highlights_images', 'motor_gallery_highlights_shortcode');
