<?php
function enqueue_motor_overview_mega_gallery_css()
{
    wp_enqueue_style('overview-mega-gallery-style', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-template-listings/overview/motor/css/mega-gallery.css', array(), '1.0', 'all');
}
// add_action('wp_enqueue_scripts', 'enqueue_overview_mega_gallery_css');

function motore_mega_gallery_shortcode()
{
    enqueue_motor_overview_mega_gallery_css();

    global $wpdb;

    $global_listing_post_data = get_motor_listing_from_query_vars();
    if (empty($global_listing_post_data) || !array($global_listing_post_data)) {
        return;
    }

    $listing_post = $global_listing_post_data['post'];
    $post_title = $listing_post->post_title;

    $variant_posts = $global_listing_post_data['variant_posts'];
    if (empty($variant_posts) || !array($variant_posts)) {
        return;
    }

    $variant_ids = array_map(function ($variant) {
        return $variant->ID;
    }, $variant_posts);

    $placeholders = implode(',', array_fill(0, count($variant_ids), '%d'));
    $sql = $wpdb->prepare(
        "SELECT colour, type, image_data FROM car_image WHERE variant_post_id IN ($placeholders)",
        ...$variant_ids
    );
    $image_data = $wpdb->get_results($sql);

    $images = [];
    foreach ($image_data as $image) {
        $imageDataArray = json_decode($image->image_data);

        if ($imageDataArray) {
            foreach ($imageDataArray as $index => $imgData) {
                // Initialize the type array if it doesn't exist
                if (!isset($images[strtolower($image->type)])) {
                    $images[strtolower($image->type)] = [];
                }

                // Add each image entry with `full`, `thumb`, and `alt` attributes
                $images[strtolower($image->type)][] = [
                    "full" => $imgData->url,
                    "thumb" => $imgData->url,
                    "alt" => ucfirst($image->type) . " Image " . ($index + 1)
                ];
            }
        }
    }
    if (empty($images)) {
        return ''; // No valid images found, return nothing
    }

    // Get count for each type
    $imageCounts = array_map('count', $images);

    // Prepare labels mapping for predefined labels
    $labels = [
        'exterior' => esc_html__('ภายนอก', 'voiture'),
        'interior' => esc_html__('Interior', 'voiture'),
        'colour' => esc_html__('สี', 'voiture')
    ];

    // Initialize the variables for formatted counts and tab buttons
    $formattedCounts = [];
    $tabsHtml = [];
    $firstTabSet = false;

    // Process each type of image
    foreach ($images as $type => $imageGroup) {
        // Determine the label
        $label = isset($labels[$type]) ? $labels[$type] : ucfirst($type);
        // Format the count for this image type
        $count = count($imageGroup);
        $formattedCounts[] = " รูป รูปถ่ายภายนอก {$count} {$label}";

        // Generate the tab button HTML (using $firstTabSet to handle the active class)
        $tabsHtml[] = sprintf(
            '<button class="custom-tab-btn-gallary %s" data-tab="%s">%s</button>',
            !$firstTabSet ? 'custom-tab-active-gallary' : '',
            esc_attr($type),
            $label
        );

        // Ensure only the first tab gets the "active" class
        $firstTabSet = true;
    }

    // Get total count
    $totalCount = array_sum($imageCounts);
    $current_year = date("Y");
    ob_start();
?>
    <div class="mega-gal-con">
        <span class="mega-gallary-title wa-title-text"><?php esc_html_e('รูปภาพ ' . $post_title, 'voiture'); ?></span>
        <div class="gallary-des">
            <span><?php esc_html_e($post_data->post_title . ' ' . $current_year . '  มีรูปภาพและรูปถ่ายทั้งหมด ' . $totalCount . ' ' . implode(', ', $formattedCounts)  . ' ตรวจสอบรูปถ่ายรถมอเตอร์ไซค์ ' . $post_data->post_title . ' ล่าสุดได้ที่นี่ รวมทั้งเครื่องยนต์ เครื่องมือวัด ไฟหน้า ใบหน้าด้านข้าง ฯลฯ กระจกมองหลัง แร็คท้าย ไฟท้าย เบาะนั่ง ท่อไอเสีย ถังน้ำมัน แป้นเหยียบ ล้อ ยาง ผ้าเบรก มือจับ', 'voiture'); ?> </span>
        </div>
        <div class="custom-tabs-header-gallary">
            <?php
            // Output the tab buttons only once
            echo implode('', $tabsHtml);
            ?>
        </div>

        <div id="gallery" class="gallery-container"></div>
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const images = <?php echo json_encode($images); ?>;
            let currentCategory = 'exterior';

            function loadGallery(category) {
                const gallery = document.getElementById('gallery');
                gallery.innerHTML = '';

                const mainContainer = document.createElement('div');
                mainContainer.className = 'main-image-container';
                const sliderContainer = document.createElement('div');
                sliderContainer.className = 'slider-container';

                const slidesHTML = images[category].map(image => `
                    <div class="slide">
                        <img src="${image.full}" alt="${image.alt}" class="main-image">
                    </div>
                `).join('');
                sliderContainer.innerHTML = slidesHTML;

                const prevButton = document.createElement('button');
                prevButton.className = 'nav-button prev';
                const nextButton = document.createElement('button');
                nextButton.className = 'nav-button next';

                const counter = document.createElement('div');
                counter.className = 'image-counter';
                counter.textContent = '1 / ' + images[category].length;

                const thumbnailContainer = document.createElement('div');
                thumbnailContainer.className = 'thumbnail-container';
                const thumbnailsHTML = images[category].map((image, index) => `
                    <img src="${image.thumb}" alt="${image.alt}" class="thumbnail ${index === 0 ? 'active' : ''}" data-index="${index}">
                `).join('');
                thumbnailContainer.innerHTML = thumbnailsHTML;

                mainContainer.appendChild(sliderContainer);
                mainContainer.appendChild(prevButton);
                mainContainer.appendChild(nextButton);
                mainContainer.appendChild(counter);
                gallery.appendChild(mainContainer);
                gallery.appendChild(thumbnailContainer);

                let currentIndex = 0;
                const totalSlides = images[category].length;
                const thumbnails = thumbnailContainer.getElementsByClassName('thumbnail');

                function updateGallery(index) {
                    currentIndex = index;
                    sliderContainer.style.transition = 'transform 0.5s ease-in-out';
                    sliderContainer.style.transform = `translateX(${-index * 100}%)`;

                    Array.from(thumbnails).forEach((thumb, i) => {
                        thumb.classList.toggle('active', i === index);
                    });

                    counter.textContent = `${index + 1} / ${totalSlides}`;

                    thumbnails[index].scrollIntoView({
                        behavior: 'smooth',
                        block: 'nearest',
                        inline: 'center'
                    });
                }

                nextButton.addEventListener('click', () => {
                    currentIndex = (currentIndex + 1) % totalSlides;
                    updateGallery(currentIndex);
                });

                prevButton.addEventListener('click', () => {
                    currentIndex = (currentIndex - 1 + totalSlides) % totalSlides;
                    updateGallery(currentIndex);
                });

                Array.from(thumbnails).forEach((thumbnail, index) => {
                    thumbnail.addEventListener('click', () => {
                        updateGallery(index);
                    });
                });

                document.addEventListener('keydown', (e) => {
                    if (e.key === 'ArrowRight') {
                        nextButton.click();
                    } else if (e.key === 'ArrowLeft') {
                        prevButton.click();
                    }
                });
            }

            loadGallery(currentCategory);

            document.querySelectorAll('.custom-tab-btn-gallary').forEach(tab => {
                tab.addEventListener('click', function() {
                    document.querySelectorAll('.custom-tab-btn-gallary').forEach(t => t.classList.remove('custom-tab-active-gallary'));
                    this.classList.add('custom-tab-active-gallary');
                    currentCategory = this.getAttribute('data-tab');
                    loadGallery(currentCategory);
                });
            });
        });
    </script>
<?php
    return ob_get_clean();
}

add_shortcode('motor_mega_image_gallery', 'motore_mega_gallery_shortcode');
