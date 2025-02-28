<?php
function single_listing_tab_videos_carousal($atts)
{
    // Arguments to get video posts
    $args = array(
        'post_type' => 'video',
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
                    <h3 style="color: #262626; text-align: start; margin-top: 10px; font-size: 14px; line-height: 1.2; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2; white-space: normal; max-height: 3em;font-family: roboto;margin-bottom: 10px;">
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
    <style>
        .single-listing-modal {
            display: none;
            position: fixed;
            z-index: 1000;
            padding-top: 100px;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            overflow: auto;
            background-color: rgba(0, 0, 0, 0.8);
        }

        .single-listing-modal-content {
            margin: auto;
            padding: 20px;
            background-color: white;
            width: 80%;
            max-width: 800px;
        }

        .single-listing-close {
            position: absolute;
            top: 15px;
            right: 35px;
            color: white;
            font-size: 40px;
            font-weight: bold;
            cursor: pointer;
        }
			.single-listing-latest-car-videos-container-gallary .slick-track {
    display: flex;
    gap: 20px !important;
 }
		}
        .single-listing-close:hover,
        .single-listing-close:focus {
            color: #999;
            text-decoration: none;
            cursor: pointer;
        }
		.single-listing-latest-car-videos-container-gallary{
			margin-left:-20px;
		}
        .slick-track {
            display: flex;
        }
		.single-listing-latest-car-videos-container-gallary .slick-slide{
			margin:0px !important;
		}
        /* Slick Previous/Next button styles */
        .slick-prev,
        .slick-next {
            background-color: #ffffff !important;
            /* Default white background */
            border-radius: 50%;
            width: 50px;
            height: 50px;
            z-index: 10;
            position: absolute;
            top: 50%;
            transform: translateY(-50%);
            display: flex;
            justify-content: center;
            align-items: center;
            box-shadow: 0 2px 5px 0 rgba(0, 0, 0, 0.15);
            transition: background-color 0.3s ease, color 0.3s ease;
            border: none;
        }

        .slick-prev:before,
        .slick-next:before {
            font-size: 20px;
            color: black !important;
            /* Black arrow */
        }

        /* Hover state */
        .slick-prev:hover,
        .slick-next:hover {
            background-color: white !important;
            /* Change to yellow on hover */
            color: white !important;
            box-shadow: 0 2px 5px 0 rgba(0, 0, 0, .15) !important;
        }

        /* Slick Dots customization */
        .single-listing-latest-car-videos-container-gallary .slick-prev {
            left: -5px;
        }

        .single-listing-latest-car-videos-container-gallary .slick-next {
            right: -12px;
        }
	@media screen and (max-width: 768px) {
		        .single-listing-latest-car-videos-container-gallary .slick-prev {
            display:none !important;
        }

        .single-listing-latest-car-videos-container-gallary .slick-next {
            display:none !important;
        }
		}
    </style>

<?php
    wp_reset_postdata(); // Reset the query
    return ob_get_clean();
}

add_shortcode('single_listing_tab_videos_carousal', 'single_listing_tab_videos_carousal');
