<?php

/**
 * Common function to extract the author ID from the current URL.
 *
 * @return int|null The author ID if found, or null otherwise.
 */

function enqueue_author_latest_news_css()
{
    wp_enqueue_style(
        'author-latest-news-style',
        get_stylesheet_directory_uri() . '/widget-shortcodes/home/css/latest-news-home.css',
        array(),
        '1.0',
        'all'
    );
}

/**
 * Shortcode to display author details.
 *
 * @param array $atts Shortcode attributes.
 *   - 'author_id': Optional. The ID of the author to display. Defaults to extracting from URL.
 *
 * @return string HTML output for author details.
 */
if (!function_exists('get_author_details_shortcode')) {
    function get_author_details_shortcode($atts)
    {
        $atts = shortcode_atts(array('author_id' => null), $atts);

        // Get author_id from query var if not provided in shortcode attributes
        $query_var_id = get_query_var('author_slug');
        $author_id = $atts['author_id'] ? intval($atts['author_id']) : intval($query_var_id);

        $author_id = get_wp_user_id_from_custom_author_id($author_id);

        if ($author_id) {
            $author_name = get_the_author_meta('display_name', $author_id);
            $author_image_url = get_the_author_meta('user_url', $author_id);
            $author_title = get_the_author_meta('title', $author_id);
            $author_description = get_the_author_meta('description', $author_id);

            if ($author_name || $author_image_url || $author_title || $author_description) {
                ob_start();
?>
                <div class="author-box">
                    <div class="author-avatar">
                        <img src="<?php echo esc_url($author_image_url); ?>" alt="<?php echo esc_attr($author_name); ?>" width="64" height="64" style="border-radius: 50%;">
                    </div>
                    <div class="author-info">
                        <h3 class="author-name">
                            <span class="author-name first-letter-capital"><?php echo esc_html($author_name); ?></span>
                            <span class="author-title first-letter-capital"><?php echo esc_html($author_title); ?></span>
                        </h3>
                        <p class="author-description"><?php echo esc_html($author_description); ?></p>
                    </div>
                </div>
                <style>
                    .author-box {
                        display: flex;
                        align-items: center;
                        border: 1px solid #ddd;
                        padding: 20px;
                        border-radius: 8px;
                        background-color: #fff;
                    }

                    .author-avatar {
                   
                        padding: 10px;
                    }

                    .author-avatar img {
                        border-radius: 50%;
                        width: 120px;
                        height: 100px;
                    }

                    .author-info {
                        flex-grow: 1;
                        padding-left: 20px;
                        border-top: none !important;
                    }

                    .author-name {
                        font-size: 20px;
                        margin: 0;
                        font-weight: 600;
                    }

                    .author-title {
                        font-size: 14px;
                        color: #777;
                        margin-left: 10px;
                    }

                    .author-description {
                        font-size: 14px;
                        font-weight: 400;
                        color: #595959;
                        line-height: 1.5;
                        white-space: pre-line;
						font-family:'Roboto'
                    }

                    .author-info .author-title {
                       
                        font-size: 14px;
                        font-weight: 400;
                        color: #8c8c8c;
                    }

                    .author-box {
                        border-radius: 0px !important;
                        padding: 0px !important;
                    }

                    .share-icon {
                        position: fixed;
                        left: 200px;
                        top: 252px;
                    }
                </style>
        <?php
                return ob_get_clean();
            }
        }
        return '<p>No author found.</p>';
    }
}
add_shortcode('author_details', 'get_author_details_shortcode');
/**
 * Shortcode to display the latest news by a specific author.
 *
 * @param array $atts Shortcode attributes.
 *   - 'author_id': Optional. The ID of the author whose news to display. Defaults to extracting from URL.
 *   - 'number': Number of posts to display. Default is 5.
 *
 * @return string HTML output of the latest news posts.
 */
if (!function_exists('get_latest_news_shortcode')) {
    function get_latest_news_shortcode($atts)
    {
        enqueue_author_latest_news_css();
        global $wpdb;

        $atts = shortcode_atts(
            array(
                'author_id' => null, // Author ID is optional.
                'number'    => 10,    // Default number of posts.
                'page'      => 1,     // Default page number.
            ),
            $atts
        );

        // Get author_id from query var if not provided in shortcode attributes
        $query_var_id = get_query_var('author_slug');
        $author_id = $atts['author_id'] ? intval($atts['author_id']) : intval($query_var_id);

        $author_id = get_wp_user_id_from_custom_author_id($author_id);

        $author_page_link = get_author_posts_url($author_id);

        $custom_author_link = get_custom_author_post_link($author_id, $author_page_link);

        if (!$author_id) {
            return '<p>No author ID provided or found in the URL.</p>';
        }

        $args = array(
            'author'         => $author_id,
            'post_type'      => 'news',
            'posts_per_page' => $atts['number'],
            'paged'          => $atts['page'],  // Add pagination here
            'meta_query' => array(
                array(
                    'key'     => 'second_language',
                    'value'   => '',
                    'compare' => '==',
                ),
            ),
            'meta_key'       => 'publish_time',
            'orderby'        => 'meta_value',
            'order'          => 'DESC',
            'meta_type'      => 'DATETIME',
        );

        $query = new WP_Query($args);

        if ($query->have_posts()) {
            $latest_news = [];
            while ($query->have_posts()) : $query->the_post();
                $news_id = get_the_ID();
                $title = get_the_title($news_id);
                $author = get_the_author_meta('display_name', get_post_field('post_author', $news_id));
                $publish_time = get_post_meta($news_id, 'publish_time', true);

                $thumbnail_id = get_post_meta($news_id, '_thumbnail_id', true);
                $image_post = get_post($thumbnail_id);
                $thumbnail_url = $image_post ? $image_post->guid : '';

                $author_image_url = get_the_author_meta('user_url', $author_id);
                if (!$author_image_url) {
                    $author_image_url = get_avatar_url($author_id, ['size' => 32]);
                }

                $latest_news[] = [
                    'id'             => $news_id,
                    'title'          => $title,
                    'author'         => $author,
                    'thumbnail_url'  => $thumbnail_url,
                    'post_date'      => convert_myt_to_ist($publish_time),
                    'avatar'         => $author_image_url,
                    'link'           => get_permalink(),
                ];
            endwhile;
            wp_reset_postdata();
        } else {
            return '<p>No news found for this author.</p>';
        }

        // Output the news
        ?>
        <div style="display: flex; align-items: center;">
            <span>
                <h2 class="wa-title-text">Latest News</h2>
            </span>
        </div>
        <ul class="latest-news-list">
            <?php foreach ($latest_news as $post_data) : ?>
                <li class="news-item">
                    <?php if ($post_data['thumbnail_url']) : ?>
                        <div class="news-thumbnail">
                            <img src="<?php echo esc_url($post_data['thumbnail_url']); ?>" alt="<?php echo $post_data['title']; ?>" />
                        </div>
                    <?php endif; ?>
                    <div class="news-content">
                        <h3><a href="<?php echo esc_url($post_data['link']); ?>" class="news-title"><?php echo $post_data['title']; ?></a></h3>
                        <div class=" news-meta-authour">
                            <div class="news-avatar-author">
                                <a href="<?php echo esc_url($custom_author_link); ?>">
                                    <span class="news-avatar">
                                        <img src="<?php echo esc_url($post_data['avatar']); ?>" />
                                        <span class="authour-name"><?php echo esc_html($post_data['author']); ?> </span>
                                    </span>
                                </a>
                            </div>
                            <span class="news-date"><?php echo esc_html($post_data['post_date']); ?></span>
                        </div>
                    </div>
                </li>
            <?php endforeach; ?>
        </ul>

        <!-- Load More Button -->
        <div class="load-more-container">
            <button class="load-more-btn" data-page="<?php echo $atts['page'] + 1; ?>" data-author-id="<?php echo $author_id; ?>">Load More News</button>
        </div>
<style>
	.load-more-container{
		text-align: center;
		width: 100%;
		height: 130px;
		display: flex;
		justify-content: center;
		align-items: center;
		margin-top: -40px;
		margin-bottom: 38px;
	}
	.load-more-btn{
		border-radius: 5px;
		border: 1px solid #32d0c6;
		font-family: "Roboto";
		color: #32d0c6;
		font-weight: 700;
		font-size: 16px;
		text-align: center;
		box-sizing: border-box;
		transition: all .2s;
		position: relative;
		background: #fff;
	}
	.news-meta-authour{
		margin-top:111px;
		display:flex;
	}
		@media screen and (max-width: 768px) {
			.news-meta-authour {
				margin-top: 20px;
				display: flex;
				justify-content: space-between;
			}
			.author-box {
				display: flex;
				align-items: center;
				border: 1px solid #ddd;
				padding: 20px;
				border-radius: 8px;
				background-color: #fff;
				flex-direction: column;
			}
	}
</style>
        <script>
            // Handle Load More button click
            document.querySelector('.load-more-btn').addEventListener('click', function() {
                var button = this;
                var page = button.getAttribute('data-page');
                var authorId = button.getAttribute('data-author-id');

                // Use AJAX to load more posts
                var data = {
                    action: 'load_more_news',
                    author_id: authorId,
                    page: page,
                };

                jQuery.post('<?php echo admin_url('admin-ajax.php'); ?>', data, function(response) {
                    if (response) {
                        document.querySelector('.latest-news-list').insertAdjacentHTML('beforeend', response);
                        button.setAttribute('data-page', parseInt(page) + 1); // Update the page number
                    }
                });
            });
        </script>
        <?php
    }
}
add_shortcode('author_latest_news', 'get_latest_news_shortcode');

// Handle Load More AJAX request
function load_more_news()
{
    if (isset($_POST['author_id']) && isset($_POST['page'])) {
        $author_id = intval($_POST['author_id']);
        $page = intval($_POST['page']);

        $author_page_link = get_author_posts_url($author_id);

        $custom_author_link = get_custom_author_post_link($author_id, $author_page_link);

        $args = array(
            'author'         => $author_id,
            'post_type'      => 'news',
            'posts_per_page' => 10,
            'paged'          => $page,
            'meta_query' => array(
                array(
                    'key'     => 'second_language',
                    'value'   => '',
                    'compare' => '==',
                ),
            ),
            'meta_key'       => 'publish_time',
            'orderby'        => 'meta_value',
            'order'          => 'DESC',
            'meta_type'      => 'DATETIME',
        );

        $query = new WP_Query($args);

        if ($query->have_posts()) {
            $latest_news = [];
            while ($query->have_posts()) : $query->the_post();
                $news_id = get_the_ID();
                $title = get_the_title($news_id);
                $author = get_the_author_meta('display_name', get_post_field('post_author', $news_id));
                $publish_time = get_post_meta($news_id, 'publish_time', true);

                $thumbnail_id = get_post_meta($news_id, '_thumbnail_id', true);
                $image_post = get_post($thumbnail_id);
                $thumbnail_url = $image_post ? $image_post->guid : '';

                $author_image_url = get_the_author_meta('user_url', $author_id);
                if (!$author_image_url) {
                    $author_image_url = get_avatar_url($author_id, ['size' => 32]);
                }

                $latest_news[] = [
                    'id'             => $news_id,
                    'title'          => $title,
                    'author'         => $author,
                    'thumbnail_url'  => $thumbnail_url,
                    'post_date'      => convert_myt_to_ist($publish_time),
                    'avatar'         => $author_image_url,
                    'link'           => get_permalink(),
                ];
            endwhile;
            wp_reset_postdata();

            foreach ($latest_news as $post_data) {
        ?>
                <li class="news-item">
                    <?php if ($post_data['thumbnail_url']) : ?>
                        <div class="news-thumbnail">
                            <img src="<?php echo esc_url($post_data['thumbnail_url']); ?>" alt="<?php echo $post_data['title']; ?>" />
                        </div>
                    <?php endif; ?>
                    <div class="news-content">
                        <h3><a href="<?php echo esc_url($post_data['link']); ?>" class="news-title"><?php echo $post_data['title']; ?></a></h3>
                        <div class="news-meta">
                            <div class="news-avatar-author">
                                <a href="<?php echo esc_url($custom_author_link); ?>">
                                    <span class="news-avatar">
                                        <img src="<?php echo esc_url($post_data['avatar']); ?>" />
                                        <span class="authour-name"><?php echo esc_html($post_data['author']); ?> </span>
                                    </span>
                                </a>
                            </div>
                            <span class="news-date"><?php echo esc_html($post_data['post_date']); ?></span>
                        </div>
                    </div>
                </li>
<?php
            }
        }
    }
    die();
}
add_action('wp_ajax_load_more_news', 'load_more_news');
add_action('wp_ajax_nopriv_load_more_news', 'load_more_news');
