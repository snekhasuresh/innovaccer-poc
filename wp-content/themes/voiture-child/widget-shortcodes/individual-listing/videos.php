<?php

add_shortcode('individual_listing_videos', 'individual_listing_videos_shortcode');
function individual_listing_videos_shortcode($atts, $content = null)
{
    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $listing_name = $make . '-' . $listing_name;

    // get listing post by post name
    $listing_posts = get_posts(array(
        'name' => $listing_name,
        'post_type' => 'listing',
        'posts_per_page' => 1
    ));

    $listing_post = $listing_posts[0];
    if ($listing_post) {

        // Your custom car video carousel logic
        $brand_id = get_post_meta($listing_post->ID, '_listing_make', true); // Get the brand ID from the post meta

        $args = array(
            'post_type'      => 'video',
            'orderby'        => 'date',
            'order'          => 'DESC',
        );

        if (!empty($brand_id)) {
            $listing_ids = wp_list_pluck(get_posts([
                'post_type'      => 'listing',
                'posts_per_page' => -1,
                'meta_query'     => [['key' => '_listing_make', 'value' => $brand_id, 'compare' => '=']]
            ]), 'ID');

            if (!empty($listing_ids)) {
                $meta_query = ['relation' => 'OR'];

                foreach ($listing_ids as $listing_id) {
                    $meta_query[] = [
                        'key'     => 'related_car_model',
                        'value'   => '"' . $listing_id . '"',
                        'compare' => 'LIKE'
                    ];
                }

                // Add meta_query to args only if listings exist
                $args['meta_query'] = $meta_query;
            }
        }

        $video_query = new WP_Query($args);
		
 		if (!$video_query->have_posts()) {
        	return;
    	}
?>
		<div class="variants-videos-title-con">
			<h2 class="variants-videos-title wa-title-text"><?php esc_html_e($listing_post->post_title, 'voiture'); ?> Xe Ô Tô Video</h2>
		</div>
        <div class="single-listing-car-videos-container single-listing-carousel">
            <?php if ($video_query->have_posts()) : ?>
                <?php while ($video_query->have_posts()) : $video_query->the_post(); ?>
                    <?php
                    $video_id = get_post_meta(get_the_ID(), 'video_youtube_id', true);
                    $video_title = get_the_title(); // Get the video title
                    ?>
                    <div class="single-listing-car-video" style="background-color: white; padding: 10px; margin-bottom: 20px; border: 1px solid #ccc; border-radius: 5px;">
                        <div class="video-overlay" style="display:flex;">
                            <iframe width="100%" height="150"
                                src="https://www.youtube.com/embed/<?php echo esc_attr($video_id); ?>?controls=1&modestbranding=1&rel=0&loop=1&playlist=<?php echo esc_attr($video_id); ?>&fs=0&iv_load_policy=3"
                                frameborder="0"
                                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                                allowfullscreen>
                            </iframe>
                        </div>
                        <div style="font-weight:700;color: 576b95;  margin-top: 10px; font-size: 14px; line-height: 1.2; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2; white-space: normal; max-height: 3em;">
                            <?php echo esc_html($video_title); ?>
                        </div>
                    </div>
                <?php endwhile; ?>
            <?php endif; ?>
        </div>

        <div id="videoModal" class="modal">
            <span class="close">&times;</span>
            <div class="modal-content">
                <iframe id="modal-video-iframe" width="100%" height="500px" src="" frameborder="0" allow="autoplay; encrypted-media" allowfullscreen></iframe>
            </div>
        </div>

        <script type="text/javascript">
            jQuery(document).ready(function($) {
                $('.single-listing-carousel').slick({
                    slidesToShow: 3,
                    slidesToScroll: 1,
                    arrows: true,
                    prevArrow: '<button class="slick-prev" aria-label="Previous" type="button">‹</button>',
                    nextArrow: '<button class="slick-next" aria-label="Next" type="button">›</button>',
                    infinite: false,
                    cssEase: 'ease',
                    responsive: [{
                            breakpoint: 1024,
                            settings: {
                                slidesToShow: 3,
                                slidesToScroll: 1
                            }
                        },
                        {
                            breakpoint: 600,
                            settings: {
                                slidesToShow: 2,
                                slidesToScroll: 1
                            }
                        },
                        {
                            breakpoint: 480,
                            settings: {
                                slidesToShow: 1,
                                slidesToScroll: 1
                            }
                        }
                    ]
                });

                // Video overlay click handler
                $('.video-overlay').on('click', function() {
                    var videoId = $(this).data('video-id');
                    var modal = $('#videoModal');
                    var iframe = $('#modal-video-iframe');
                    iframe.attr('src', 'https://www.youtube.com/embed/' + videoId + '?autoplay=1');
                    modal.show();
                });

                // Close the modal
                $('.close').on('click', function() {
                    var modal = $('#videoModal');
                    var iframe = $('#modal-video-iframe');
                    iframe.attr('src', '');
                    modal.hide();
                });

                // Close modal when clicking outside content
                $(window).on('click', function(event) {
                    var modal = $('#videoModal');
                    if (event.target == modal[0]) {
                        var iframe = $('#modal-video-iframe');
                        iframe.attr('src', '');
                        modal.hide();
                    }
                });
            });
        </script>

        <style>
            /* Modal styling */
            .modal {
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

            .modal-content {
                margin: auto;
                padding: 20px;
                background-color: white;
                width: 80%;
                max-width: 800px;
            }

            .close {
                position: absolute;
                top: 15px;
                right: 35px;
                color: white;
                font-size: 40px;
                font-weight: bold;
                cursor: pointer;
            }

            .close:hover,
            .close:focus {
                color: #999;
                text-decoration: none;
                cursor: pointer;
            }

            .slick-track {
                display: flex;
                gap: 10px;
            }

            .slick-prev,
            .slick-next {
                background-color: #ffffff !important;
                border-radius: 50%;
                width: 50px;
                height: 50px;
                z-index: 10;
                top: 50%;
                transform: translateY(-50%);
                display: flex;
                justify-content: center;
                align-items: center;
                box-shadow: 0 2px 5px 0 rgba(0, 0, 0, 0.15);
                border: none;
            }

            .slick-prev:before,
            .slick-next:before {
                font-size: 20px;
                color: black !important;
            }

            .slick-prev {
                left: -25px;
            }

            .slick-next {
                right: -25px;
            }
        </style>

<?php
        wp_reset_postdata();
    }
}
