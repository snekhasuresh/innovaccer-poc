<?php
function enqueue_motor_gallery_videos_css()
{
    wp_enqueue_style('gallery-videos-style', get_stylesheet_directory_uri() . '/widget-shortcodes/motor-template-listings/motor-gallery/motor/css/gallery-videos.css', array(), '1.0', 'all');
}

function single_listing_motor_tab_videos_carousal($atts)
{
    enqueue_motor_gallery_videos_css();
    // Arguments to get video posts
    $args = array(
        'post_type' => 'motocycle-video',
        'posts_per_page' => 4 // Show 4 videos initially for the carousel
    );
    $video_query = new WP_Query($args);

    ob_start();
?>
    <h2 class="wa-title-text">Videos</h2>

    <div class="single-listing-latest-car-videos-container-gallary">
		 <div class="single-listing-latest-car-videos-container single-listing-ev-comparison-carousel">
        <?php if ($video_query->have_posts()): ?>
            <?php while ($video_query->have_posts()):
                $video_query->the_post();
                $video_id = get_post_meta(get_the_ID(), 'video_youtube_id', true);
                $video_title = get_the_title(); // Get the video title
            ?>

                <div class="single-listing-latest-car-video" style="background-color: white; padding: 10px; margin-bottom: 20px; border: 1px solid #ccc; border-radius: 5px;">
                    <div class="single-listing-video-overlay" style="display:flex;">
                        <iframe width="150" height="150"
                            src="https://www.youtube.com/embed/<?php echo esc_attr($video_id); ?>?controls=1&modestbranding=1&rel=0&loop=1&playlist=<?php echo esc_attr($video_id); ?>&fs=0&iv_load_policy=3"
                            frameborder="0"
                            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                            allowfullscreen>
                        </iframe>
                    </div>
                    <h3 style="color: black; text-align: center; margin-top: 10px; font-size: 14px; line-height: 1.2; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2; white-space: normal; max-height: 3em; margin-bottom: 0px;">
                        <?php echo esc_html($video_title); ?>
                    </h3>
                </div>
            <?php endwhile; ?>
        <?php endif; ?>
    </div>
</div>
    <div id="single-listing-videoModal" class="single-listing-modal">
        <span class="single-listing-close">&times;</span>
        <div class="single-listing-modal-content">
            <iframe id="single-listing-modal-video-iframe" width="100%" height="500px" src="" frameborder="0" allow="autoplay; encrypted-media" allowfullscreen></iframe>
        </div>
    </div>
    <script type="text/javascript">
        jQuery(document).ready(function($) {
            $('.single-listing-ev-comparison-carousel').slick({
                slidesToShow: 3, // Shows 3 videos initially
                slidesToScroll: 1,
                arrows: true,
                prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                infinite: false, // Prevents looping
                cssEase: 'ease', // Smooth scrolling
                responsive: [{
                        breakpoint: 1024,
                        settings: {
                            slidesToShow: 3,
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
                  
                ]
            });
        });

        $('.single-listing-video-overlay').on('click', function() {
            var videoId = $(this).data('video-id');
            var modal = $('#single-listing-videoModal');
            var iframe = $('#single-listing-modal-video-iframe');

            // Open modal and set video source
            iframe.attr('src', 'https://www.youtube.com/embed/' + videoId + '?autoplay=1');
            modal.show();
        });

        // Close the modal
        $('.single-listing-close').on('click', function() {
            var modal = $('#single-listing-videoModal');
            var iframe = $('#single-listing-modal-video-iframe');

            // Stop the video
            iframe.attr('src', '');
            modal.hide();
        });

        // Close modal when clicking outside content
        $(window).on('click', function(event) {
            var modal = $('#single-listing-videoModal');
            if (event.target == modal[0]) {
                var iframe = $('#single-listing-modal-video-iframe');
                iframe.attr('src', '');
                modal.hide();
            }
        });
    </script>
<?php
    wp_reset_postdata(); // Reset the query
    return ob_get_clean();
}

add_shortcode('single_listing_motor_tab_videos_carousal', 'single_listing_motor_tab_videos_carousal');
