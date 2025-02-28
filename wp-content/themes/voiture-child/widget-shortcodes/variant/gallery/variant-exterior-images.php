<?php

function variant_gallery_exterior_shortcode()
{
    global $wpdb;

    $global_variant_post_data = get_variant_from_query_vars();
    if (!$global_variant_post_data) {
        return;
    }

    $current_variant_post = $global_variant_post_data['variant_post'];
    $current_variant_post_title = $current_variant_post->post_title;
    $current_variant_id = $current_variant_post->ID;

    $sql = $wpdb->prepare(
        "SELECT colour, type, image_data FROM car_image WHERE variant_post_id=%d",
        $current_variant_id
    );

    $data = $wpdb->get_results($sql);

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
                    'alt' => $current_variant_post_title . ' Exterior ' . str_pad($exteriorIndex++, 3, '0', STR_PAD_LEFT),
                    'title' => $current_variant_post_title . ' Exterior ' . str_pad($exteriorIndex - 1, 3, '0', STR_PAD_LEFT),
                    'link' => '/cars/honda/hr-v/car-exterior-image-' . $exteriorIndex
                ];
            }
        }
    }

    $totalImages = count($images);
    $initialImages = array_slice($images, 0, 9); // Initial 9 images

    // Start building the output
    ob_start();
?>
    <h2 class="wa-title-text"><?php esc_html_e($current_variant_post_title . ' Exterior Images', 'voiture'); ?></h2>

    <div class="gallery-list" data-total-images="<?php echo esc_attr($totalImages); ?>" data-listing-name="<?php echo esc_attr($current_variant_post_title); ?>">
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
                View More
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                </svg>
            </button>
        </div>
    <?php endif; ?>

    <style>
        .view-more {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 5px;
            margin: 24px auto 0;
            padding: 8px 16px;
            border: none;
            background: none;
            color: #576B95;
            font-size: 16px;
            font-weight: 700;
            cursor: pointer;
        }

        .view-more svg {
            width: 18px;
            height: 20px;
        }

        .gallery-list {
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
        }

        .gallery-list-item {
            width: calc(33.33% - 20px);
            background: #fff;
            border-radius: 2px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        }

        .gallery-list-item img {
            width: 100%;
            height: 165px;
            display: block;
            border-bottom: 2px solid #ddd;
        }

        .description-link {
            display: block;
            text-align: center;
            padding: 10px;
            font-size: 12px;
            font-weight: bold;
            color: #333;
            text-decoration: none;

            /* Overflow ellipsis for text */
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .description-link:hover {
            color: #32D0C6;
        }

        .view-more-container {
            text-align: center;
            margin-top: 20px;
        }

        .view-more-link {
            padding: 10px 20px;
            background-color: #32D0C6;
            color: #fff;
            text-decoration: none;
            border-radius: 4px;
            font-weight: bold;
        }

        .view-more-link:hover {
            background-color: #28b9a7;
        }
    </style>

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
add_shortcode('variant_exterior_images', 'variant_gallery_exterior_shortcode');
