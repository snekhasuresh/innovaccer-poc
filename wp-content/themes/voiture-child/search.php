<?php

/**
 * The template for displaying search results pages.
 *
 * @package WordPress
 * @subpackage Voiture
 * @since Voiture 1.0
 */

get_header();
$sidebar_configs = voiture_get_blog_layout_configs();

$columns = voiture_get_config('blog_columns', 1);
$bscol = floor(12 / $columns);

voiture_render_breadcrumbs();
?>
<section id="main-container" class="main-content  <?php echo apply_filters('voiture_blog_content_class', 'container'); ?> inner">

    <a href="javascript:void(0)" class="mobile-sidebar-btn hidden-lg hidden-md"> <i class="fa fa-bars"></i></a>
    <div class="mobile-sidebar-panel-overlay"></div>
    <div class="row responsive-medium">
        <div id="main-content" >
            <main id="main"  class="site-main layout-blog col-xs-12 <?php echo esc_attr(is_active_sidebar('sidebar-default') ? 'col-md-8 col-lg-9' : 'col-md-12'); ?>" role="main">
				<?php
                if (isset($_GET['s']) && !empty($_GET['s'])) {
                    // Get the search text from the 's' query parameter
                    $search_text = sanitize_text_field($_GET['s']);
                }
                ?>

                <?php if (have_posts()) : ?>

                    <header class="page-header hidden">
                        <?php
                        the_archive_title('<h1 class="page-title">', '</h1>');
                        the_archive_description('<div class="taxonomy-description">', '</div>');
                        ?>
                    </header><!-- .page-header -->
					<?php echo do_shortcode('[breadcrumb]'); ?>
					<h1>Search result for ' <?php echo $search_text ?>'</h1>
                    <div class="layout-posts-list">
                        <?php
                        // Start the Loop.
                        while (have_posts()) : the_post();

                            /*
						 * Include the Post-Format-specific template for the content.
						 * If you want to override this in a child theme, then include a file
						 * called content-___.php (where ___ is the Post Format name) and that will be used instead.
						 */
                            global $post;
                            if ($post->post_type == 'product') {
                                get_template_part('woocommerce/item-product/inner', 'search');
                            } else {
                                $thumbnail_id = get_post_meta($post->ID, '_thumbnail_id', true);
                                $image_post = get_post($thumbnail_id);
                                $guid = $image_post->guid;

                                $post_id = get_the_ID(); // Get post ID
                                $thumbnail_url = $guid; // Get thumbnail URL
                                $author_name = get_the_author(); // Get author name
                                $comment_count = get_comments_number(); // Get comment count
                                $publish_date = get_the_date(); // Get publish date
                                $news_url = get_custom_post_link(get_the_ID());
                        ?>

                                 <div class="post-card">
                                    <a class="thumbnail" href="<?php echo $news_url; ?>">
                                        <?php if ($thumbnail_url) : ?>
                                            <img src="<?php echo esc_url($thumbnail_url); ?>" alt="<?php the_title(); ?>" style="width:300px; border-radius: 8px;">
                                        <?php else : ?>
                                            <img src="<?php echo esc_url(get_template_directory_uri() . '/images/placeholder.png'); ?>" alt="Placeholder" style="width: 100%; border-radius: 8px;">
                                        <?php endif; ?>
                                    </a>
                                    <div class="content" style="flex: 3; padding-left: 15px;">
                                        <div class="meta" style="font-size: 14px; color: #8c8c8c;font-family: 'Roboto'; margin-bottom: 8px;">
                                            <span><?php echo esc_html($author_name); ?></span> |
                                            <span><?php echo esc_html($comment_count); ?> Comments</span> |
                                            <span><?php echo esc_html($publish_date); ?></span>
                                        </div>
                                        <h2 class="news-title-search">
                                            <a href="<?php echo $news_url; ?>" style="text-decoration: none; color: #333;"><?php the_title(); ?></a>
                                        </h2>
                                        <p class="news-summary-search"><?php echo wp_trim_words(get_the_excerpt(), 30, '...'); ?></p>
                                        <a href="<?php echo $news_url; ?>" style="display: inline-block; font-family: 'Roboto'; color: #262626; border-radius: 4px; font-size: 14px;font-weight: 700;">
                                            Read More
                                        </a>
                                    </div>
                                </div>

                        <?php
                                // get_template_part('content', 'search');
                            }
                        // End the loop.
                        endwhile; ?>
                    </div>
                <?php
                    // Previous/next page navigation.
                    voiture_paging_nav();

                // If no content, include the "No posts found" template.
                else :
                    get_template_part('template-posts/content', 'none');

                endif;
                ?>
				
                <?php echo do_shortcode('[popular_car_brands]'); ?>

            </main><!-- .site-main -->
			<div class="col-xs-12 col-md-4 col-lg-3">
                <?php echo do_shortcode('[recommended_cars]'); ?>
                <?php echo do_shortcode('[popular_car_videos]'); ?>
            </div>
        </div><!-- .content-area -->
    </div>
</section>
<style>
    .thumbnail {
        margin-bottom: 0px !important;
    }
.post-card {
    display: flex;
    border: 1px solid #ddd;
    padding: 15px;
    margin: 15px 0;
    border-radius: 8px;
    box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
}
    .thumbnail img {
        width: 300px !important;
        height: 100%;
    }

    .news-title-search {
        font-family: "Roboto Condensed";
        font-weight: 700;
        font-size: 26px;
        color: #262626;
        line-height: 32px;
        overflow: hidden;
        text-overflow: ellipsis;
        display: -webkit-box;
        -webkit-line-clamp: 2;
        -webkit-box-orient: vertical;
        margin-bottom: 6px;
        margin-top: 0px;
    }

    .row.responsive-medium {
        padding-top: 0px !important;
    }

    .news-summary-search {
        font-family: Roboto;
        font-size: 14px;
        color: #8c8c8c;
        line-height: 20px;
        overflow: hidden;
        text-overflow: ellipsis;
        display: -webkit-box;
        -webkit-line-clamp: 2;
        -webkit-box-orient: vertical;
    }
	@media screen and (max-width: 768px) {
		.post-card {
		    display: flex;
			flex-direction: column;
			border: 1px solid #ddd;
			padding: 15px;
			margin: 15px 0;
			border-radius: 8px;
			box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
		}
		.post-card .thumbnail{
			width:100% !important;
		}
		.post-card .thumbnail img {
			width: 100% !important;
			height: 100%;
		}
	}
</style>
<?php get_footer(); ?>