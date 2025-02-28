<?php
function enqueue_find_new_motor_videos_css()
{
    // Register and enqueue the CSS file
    wp_enqueue_style('find-new-motor-videos-style', get_stylesheet_directory_uri() . '/widget-shortcodes/new-cars/css/find-new-cars-videos.css', array(), '1.0', 'all');
}
// add_action('wp_enqueue_scripts', 'enqueue_find_new_cars_videos_css');

function findnew_motor_videos_carousal($atts)
{
    enqueue_find_new_motor_videos_css();

    $atts = shortcode_atts(array(
        'brand_id' => 0,
        'brand_name' => '',
    ), $atts);

    $brand_id = $atts['brand_id'];

    $videos = get_motor_videos_data($brand_id);
    $video_posts = $videos['posts'];
    $brand_name = $videos['brand_name'];

    if ($video_posts === null || empty($video_posts)) {
        return '';
    }

    ob_start();

    if ($brand_name) {
        echo '<h2 class="wa-title-text find-new-car-video-title">วีดีโอ รถมอเตอร์ไซค์ ' . $brand_name . ' ในไทย</h2>';
    } else {
        echo '<h2 class="wa-title-text find-new-car-video-title">วีดีโอรถมอเตอร์ไซค์ใหม่ล่าสุดในไทย</h2>';
    }

?>

    <div class="latest-car-videos-container ev-comparison-carousel">

        <?php foreach ($video_posts as $video) :
            $video_id = $video->video_youtube_id;
            $video_title = $video->post_title;
        ?>

            <div class="latest-car-video" style="background-color: white; padding: 10px; margin-bottom: 20px; border: 1px solid #ccc; border-radius: 5px;">
                <div class="video-overlay" style="display:flex; position: relative; cursor: pointer;" data-video-id="<?php echo esc_attr($video_id); ?>">
                    <img src="https://img.youtube.com/vi/<?php echo esc_attr($video_id); ?>/hqdefault.jpg"
                        alt="Video Thumbnail" class="youtube-thumbnail" loading="lazy">
                    <!-- Play Button -->
                    <img src="https://img.wapcar.my/assets/play-button.png"
                        alt="Play Button" class="play-button"
                        style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); width: 50px; height: 50px;">
                </div>

                <h3 style="text-align: start; margin-top: 10px; font-size: 14px; line-height: 1.2; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2; white-space: normal; max-height: 3em;font-family: 'Roboto';font-weight: 700;color: #262626;margin-bottom: 8px;">
                    <?php echo esc_html($video_title); ?>
                </h3>
            </div>
        <?php endforeach; ?>
    </div>
    <div id="videoModal" class="modal">
        <span class="close">&times;</span>
        <div class="modal-content">
            <iframe id="modal-video-iframe" width="100%" height="500px" src="" frameborder="0" allow="autoplay; encrypted-media" allowfullscreen></iframe>
        </div>
    </div>
    <script type="text/javascript">
        document.addEventListener("DOMContentLoaded", function() {
            // For each video container, add a mouseenter event to load the video.
            document.querySelectorAll(".video-overlay").forEach(function(video) {
                video.addEventListener("mouseenter", function() {
                    let videoId = this.getAttribute("data-video-id");
                    // If the iframe isn’t already loaded, insert it.
                    if (!this.querySelector("iframe")) {
                        this.innerHTML = `<iframe width="100%" height="204" 
                    src="https://www.youtube.com/embed/${videoId}?autoplay=1&mute=1&controls=1&modestbranding=1&rel=0"
                    frameborder="0"
                    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                    allowfullscreen>
                </iframe>`;
                    }
                });
                // When the mouse leaves the container, revert to the thumbnail image.
                video.addEventListener("mouseleave", function() {
                    let videoId = this.getAttribute("data-video-id");
                    this.innerHTML = `
                <img src="https://img.youtube.com/vi/${videoId}/hqdefault.jpg" 
                    alt="Video Thumbnail" class="youtube-thumbnail" loading="lazy" style="width: 100%; border-radius: 5px;">
                <img src="https://img.wapcar.my/assets/play-button.png" 
                    alt="Play Button" class="play-button" 
                    style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); width: 50px; height: 50px;">
            `;
                });
                // Note: No mouseleave event is added—once the video starts, it stays.
            });
        });

        // Modal functionality (remains unchanged)
        jQuery(document).ready(function($) {
            $('.ev-comparison-carousel').slick({
                slidesToShow: 3,
                slidesToScroll: 1,
                arrows: true,
                prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                infinite: false,
                cssEase: 'ease',
                responsive: [{
                        breakpoint: 1030,
                        settings: {
                            slidesToShow: 2,
                            slidesToScroll: 1
                        }
                    },
                    {
                        breakpoint: 768,
                        settings: {
                            slidesToShow: 1.2,
                            slidesToScroll: 1
                        }
                    },
                    {
                        breakpoint: 480,
                        settings: {
                            slidesToShow: 1.2,
                            slidesToScroll: 1
                        }
                    }
                ]
            });
            // Click-to-Expand: When a video is clicked, open it in the modal.
            $('.video-overlay').on('click', function() {
                var videoId = $(this).data('video-id');
                var modal = $('#videoModal');
                var iframe = $('#modal-video-iframe');
                // Set the iframe source with autoplay enabled.
                iframe.attr('src', 'https://www.youtube.com/embed/' + videoId + '?autoplay=1');
                modal.show();
            });
        });

        // Open the modal when the video container is clicked.
        jQuery('.video-overlay').on('click', function() {
            var videoId = jQuery(this).data('video-id');
            var modal = jQuery('#videoModal');
            var iframe = jQuery('#modal-video-iframe');
            // Set the video URL with autoplay enabled.
            iframe.attr('src', 'https://www.youtube.com/embed/' + videoId + '?autoplay=1');
            modal.show();
        });

        // Close the modal when the close button is clicked.
        jQuery('.close').on('click', function() {
            var modal = jQuery('#videoModal');
            var iframe = jQuery('#modal-video-iframe');
            // Stop the video.
            iframe.attr('src', '');
            modal.hide();
        });

        // Close modal when clicking outside the modal content.
        jQuery(window).on('click', function(event) {
            var modal = jQuery('#videoModal');
            if (event.target == modal[0]) {
                var iframe = jQuery('#modal-video-iframe');
                iframe.attr('src', '');
                modal.hide();
            }
        });
    </script>

<?php
    wp_reset_postdata(); // Reset the query
    return ob_get_clean();
}

add_shortcode('findnew_motor_videos_carousal', 'findnew_motor_videos_carousal');
